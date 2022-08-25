<?php
declare(strict_types=1);

namespace LessQueue;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use LessDatabase\Query\Builder\Applier\PaginateApplier;
use LessDatabase\Query\Builder\Applier\Values\InsertValuesApplier;
use LessQueue\Job\Job;
use LessQueue\Job\Property\Identifier;
use LessQueue\Job\Property\Name;
use LessValueObject\Composite\Paginate;
use LessValueObject\Number\Exception\MaxOutBounds;
use LessValueObject\Number\Exception\MinOutBounds;
use LessValueObject\Number\Exception\PrecisionOutBounds;
use LessValueObject\Number\Int\Date\Timestamp;
use LessValueObject\Number\Int\Unsigned;
use LessValueObject\String\Exception\TooLong;
use LessValueObject\String\Exception\TooShort;
use LessValueObject\String\Format\Exception\NotFormat;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use RuntimeException;

final class RabbitMqQueue implements Queue
{
    private ?AMQPChannel $channel = null;

    private const QUEUE = 'less.queue';
    private const EXCHANGE = 'base_exchange';
    private const TABLE = 'buried_queue';

    public function __construct(
        private readonly AMQPStreamConnection $connection,
        private readonly Connection $database,
    ) {}

    public function publish(Name $name, array $data, ?Timestamp $until = null): void
    {
        $this->put(
            serialize(
                [
                    'name' => $name,
                    'data' => $data,
                    'attempt' => 1,
                ],
            ),
            $until,
        );
    }

    public function republish(Job $job, ?Timestamp $until = null): void
    {
        $this->put(
            serialize(
                [
                    'name' => $job->name,
                    'data' => $job->data,
                    'attempt' => $job->attempt->getValue() + 1,
                ],
            ),
            $until,
        );
    }

    private function put(string $body, ?Timestamp $until = null): void
    {
        $message = new AMQPMessage(
            $body,
            ['delivery_mode' => 2],
        );

        if ($until && $until->getValue() >= time()) {
            $headers = new AMQPTable(['x-delay' => ($until->getValue() - time()) * 1000]);
            $message->set('application_headers', $headers);
        }

        $this->getChannel()->basic_publish($message, self::EXCHANGE);
    }

    public function process(callable $callback, Vo\Timeout $timeout): void
    {
        $till = time() + $timeout->getValue();

        $this->getChannel()->basic_consume(
            self::QUEUE,
            callback: function (AMQPMessage $message) use ($callback) {
                $decoded = unserialize($message->body);
                assert(is_array($decoded));

                assert($decoded['name'] instanceof Name);
                assert(is_array($decoded['data']));
                assert(is_int($decoded['attempt']));

                $callback(
                    new Job(
                        new Identifier($message->getDeliveryTag()),
                        $decoded['name'],
                        $decoded['data'],
                        new Unsigned($decoded['attempt']),
                    ),
                );
            },
        );

        while ($till > time()) {
            $ttl = $till - time();

            try {
                $this->getChannel()->wait(timeout: $ttl);
            } catch (AMQPTimeoutException) {
                break;
            }
        }
    }

    public function delete(Job $job): void
    {
        $this->getChannel()->basic_ack($job->id->getValue());
    }

    /**
     * @throws Exception
     */
    public function bury(Job $job): void
    {
        $applier = new InsertValuesApplier(
            [
                'job_name' => $job->name,
                'job_data' => serialize($job->data),
                'job_attempt' => $job->attempt,
            ],
        );
        $applier
            ->apply($this->database->createQueryBuilder())
            ->insert(self::TABLE)
            ->executeStatement();

        $this->delete($job);
    }

    /**
     * @throws Exception
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws NotFormat
     * @throws PrecisionOutBounds
     * @throws TooLong
     * @throws TooShort
     */
    public function reanimate(Identifier $id, ?Timestamp $until = null): void
    {
        $selectBuilder = $this->database->createQueryBuilder();
        $result = $selectBuilder
            ->addSelect('id')
            ->addSelect('job_name')
            ->addSelect('job_data')
            ->addSelect('job_attempt')
            ->from(self::TABLE)
            ->andWhere('id = :id')
            ->setParameter('id', $id)
            ->fetchAssociative();

        if ($result === false) {
            throw new RuntimeException();
        }

        $job = $this->hydrate($result);
        $this->republish($job, $until);

        $deleteBuilder = $this->database->createQueryBuilder();
        $deleteBuilder
            ->delete(self::TABLE)
            ->andWhere('id = :id')
            ->setParameter('id', $id)
            ->executeStatement();
    }

    /**
     * @throws Exception
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws PrecisionOutBounds
     * @throws TooLong
     * @throws TooShort
     * @throws NotFormat
     */
    public function getBuried(Paginate $paginate): array
    {
        $builder = $this->database->createQueryBuilder();
        (new PaginateApplier($paginate))->apply($builder);

        $results = $builder
            ->addSelect('id')
            ->addSelect('job_name')
            ->addSelect('job_data')
            ->addSelect('job_attempt')
            ->from(self::TABLE)
            ->fetchAllAssociative();

        return array_map(
            $this->hydrate(...),
            $results,
        );
    }

    /**
     * @param array<mixed> $result
     *
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws NotFormat
     * @throws PrecisionOutBounds
     * @throws TooLong
     * @throws TooShort
     */
    private function hydrate(array $result): Job
    {
        assert(is_int($result['id']));
        assert(is_string($result['job_name']));
        assert(is_string($result['job_data']));
        assert(is_int($result['job_attempt']));

        $unserialized = unserialize($result['job_data']);
        assert(is_array($unserialized));

        return new Job(
            new Identifier($result['id']),
            new Name($result['job_name']),
            $unserialized,
            new Unsigned($result['job_attempt']),
        );
    }

    private function getChannel(): AMQPChannel
    {
        if (!$this->channel instanceof AMQPChannel) {
            $this->channel = $this->connection->channel();

            $this
                ->channel
                ->exchange_declare(
                    self::EXCHANGE,
                    'x-delayed-message',
                    durable: true,
                    auto_delete: false,
                    arguments: new AMQPTable(
                        ['x-delayed-type' => AMQPExchangeType::FANOUT],
                    ),
                );

            $this
                ->channel
                ->queue_declare(
                    self::QUEUE,
                    auto_delete: false,
                    arguments: new AMQPTable(
                        ['x-dead-letter-exchange' => 'delayed'],
                    ),
                );

            $this->channel->queue_bind(self::QUEUE, self::EXCHANGE);
        }

        return $this->channel;
    }
}

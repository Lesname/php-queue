<?php
declare(strict_types=1);

namespace LessQueue;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use ErrorException;
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
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use RuntimeException;

final class RabbitMqQueue implements Queue
{
    private ?AMQPChannel $channel = null;

    private const QUEUE = 'less.queue';
    private const EXCHANGE = 'base_exchange';

    private const TABLE = 'queue_job_buried';

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

    /**
     * @throws ErrorException
     */
    public function process(callable $callback): void
    {
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
                        new Identifier('rm-' . $message->getDeliveryTag()),
                        $decoded['name'],
                        $decoded['data'],
                        new Unsigned($decoded['attempt']),
                    ),
                );
            },
        );

        $this->getChannel()->consume();
    }

    public function isProcessing(): bool
    {
        return $this->channel instanceof AMQPChannel
            && $this->getChannel()->is_consuming();
    }

    public function stopProcessing(): void
    {
        if (!$this->isProcessing()) {
            throw new RuntimeException();
        }

        $this->getChannel()->stopConsume();
    }

    public function countProcessing(): int
    {
        $result = $this
            ->getChannel()
            ->queue_declare(
                self::QUEUE,
                passive: true,
            );

        assert(is_array($result));
        assert(is_int($result[2]));

        return $result[2];
    }

    public function countProcessable(): int
    {
        $result = $this
            ->getChannel()
            ->queue_declare(
                self::QUEUE,
                passive: true,
            );

        assert(is_array($result));
        assert(is_int($result[1]));

        return $result[1];
    }

    /**
     * @throws Exception
     */
    public function delete(Identifier | Job $item): void
    {
        $id = $item instanceof Job
            ? $item->id
            : $item;

        $idType = substr($id->getValue(), 0, 2);
        $idValue = substr($id->getValue(), 3);

        if ($idType === 'rm') {
            $this->getChannel()->basic_ack((int)$idValue);
        } elseif ($idType === 'db') {
            $builder = $this->database->createQueryBuilder();
            $builder
                ->delete(self::TABLE)
                ->andWhere('id = :id')
                ->setParameter('id', $idValue)
                ->executeStatement();
        } else {
            throw new RuntimeException("Type '{$idType}' unknown");
        }
    }

    /**
     * @throws Exception
     */
    public function bury(Job $job): void
    {
        $applier = new InsertValuesApplier(
            [
                'name' => $job->name,
                'data' => serialize($job->data),
                'attempt' => $job->attempt,
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
        $dbId = substr($id->getValue(), 3);

        $selectBuilder = $this->database->createQueryBuilder();
        $result = $selectBuilder
            ->addSelect('concat("db-", id)')
            ->addSelect('name')
            ->addSelect('data')
            ->addSelect('attempt')
            ->from(self::TABLE)
            ->andWhere('id = :id')
            ->setParameter('id', $dbId)
            ->fetchAssociative();

        if ($result === false) {
            throw new RuntimeException();
        }

        $job = $this->hydrate($result);
        $this->republish($job, $until);
        $this->delete($job);
    }

    /**
     * @throws Exception
     */
    public function getBuried(Paginate $paginate): array
    {
        $builder = $this->database->createQueryBuilder();
        (new PaginateApplier($paginate))->apply($builder);

        $results = $builder
            ->addSelect('concat("db-", id)')
            ->addSelect('name')
            ->addSelect('data')
            ->addSelect('attempt')
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
        assert(is_string($result['id']));
        assert(is_string($result['name']));
        assert(is_string($result['data']));
        assert(is_int($result['attempt']));

        $unserialized = unserialize($result['data']);
        assert(is_array($unserialized));

        return new Job(
            new Identifier($result['id']),
            new Name($result['name']),
            $unserialized,
            new Unsigned($result['attempt']),
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

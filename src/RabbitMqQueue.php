<?php
declare(strict_types=1);

namespace LesQueue;

use Override;
use LesQueue\Response\Jobs;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use ErrorException;
use LesQueue\Exception\DecodeFailed;
use LesValueObject\Number\Exception\NotMultipleOf;
use LesDatabase\Query\Builder\Applier\PaginateApplier;
use LesValueObject\Composite\DynamicCompositeValueObject;
use LesDatabase\Query\Builder\Applier\Values\InsertValuesApplier;
use LesQueue\Job\Job;
use LesQueue\Job\Property\Identifier;
use LesQueue\Job\Property\Name;
use LesQueue\Parameter\Priority;
use LesValueObject\Composite\Paginate;
use LesValueObject\Number\Exception\MaxOutBounds;
use LesValueObject\Number\Exception\MinOutBounds;
use LesValueObject\Number\Int\Date\Timestamp;
use LesValueObject\Number\Int\Unsigned;
use LesValueObject\String\Exception\TooLong;
use LesValueObject\String\Exception\TooShort;
use LesValueObject\String\Format\Exception\NotFormat;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use RuntimeException;

final class RabbitMqQueue implements Queue
{
    private ?AMQPChannel $channel = null;

    private const string QUEUE = 'les.queue';
    private const string EXCHANGE = 'base_exchange';

    private const string TABLE = 'queue_job_buried';

    public function __construct(
        private readonly AMQPStreamConnection $connection,
        private readonly Connection $database,
    ) {}

    #[Override]
    public function publish(Name $name, DynamicCompositeValueObject $data, ?Timestamp $until = null, ?Priority $priority = null): void
    {
        $this->put(
            serialize(
                [
                    'name' => $name,
                    'data' => $data,
                    'attempt' => 0,
                ],
            ),
            $until,
            $priority,
        );
    }

    #[Override]
    public function republish(Job $job, Timestamp $until, ?Priority $priority = null): void
    {
        $this->put(
            serialize(
                [
                    'name' => $job->name,
                    'data' => $job->data,
                    'attempt' => $job->attempt->value + 1,
                ],
            ),
            $until,
            $priority,
        );
    }

    private function put(string $body, ?Timestamp $until = null, ?Priority $priority = null): void
    {
        $message = new AMQPMessage(
            $body,
            [
                'priority' => ($priority ?? Priority::normal())->value,
                'delivery_mode' => 2,
            ],
        );

        if ($until && $until->value >= time()) {
            $headers = new AMQPTable(['x-delay' => ($until->value - time()) * 1000]);
            $message->set('application_headers', $headers);
        }

        $this->getChannel()->basic_publish($message, self::EXCHANGE);
    }

    /**
     * @throws ErrorException
     */
    #[Override]
    public function process(callable $callback): void
    {
        $this->getChannel()->basic_consume(
            self::QUEUE,
            callback: function (AMQPMessage $message) use ($callback) {
                $callback($this->makeJob($message));
            },
        );

        $this->getChannel()->consume();
    }

    /**
     * @throws DecodeFailed
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws NotMultipleOf
     * @throws TooLong
     * @throws TooShort
     */
    private function makeJob(AMQPMessage $message): Job
    {
        $id = new Identifier('rm-' . $message->getDeliveryTag());
        $decoded = unserialize($message->getBody());

        if (!is_array($decoded)) {
            throw new DecodeFailed($id);
        }

        if (!$decoded['name'] instanceof Name) {
            throw new DecodeFailed($id);
        }

        if (!is_int($decoded['attempt'])) {
            throw new DecodeFailed($id);
        }

        if (!$decoded['data'] instanceof DynamicCompositeValueObject) {
            throw new DecodeFailed($id);
        }

        return new Job(
            $id,
            $decoded['name'],
            $decoded['data'],
            new Unsigned($decoded['attempt']),
        );
    }

    #[Override]
    public function isProcessing(): bool
    {
        return $this->channel instanceof AMQPChannel
            && $this->getChannel()->is_consuming();
    }

    #[Override]
    public function stopProcessing(): void
    {
        if (!$this->isProcessing()) {
            throw new RuntimeException();
        }

        $this->getChannel()->stopConsume();
    }

    #[Override]
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

    #[Override]
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
     * @return int<0, max>
     *
     * @throws Exception
     */
    private function countBuried(): int
    {
        $result = $this
            ->database
            ->createQueryBuilder()
            ->select('count(*)')
            ->from(self::TABLE)
            ->fetchOne();

        if ($result === false) {
            throw new RuntimeException();
        }

        if (is_string($result) && ctype_digit($result)) {
            $result = (int) $result;
        }

        if (!is_int($result)) {
            throw new RuntimeException();
        }

        if ($result < 0) {
            throw new RuntimeException();
        }

        return $result;
    }

    /**
     * @throws Exception
     */
    #[Override]
    public function delete(Identifier | Job $item): void
    {
        $id = $item instanceof Job
            ? $item->id
            : $item;

        $idType = substr($id->value, 0, 2);
        $idValue = substr($id->value, 3);

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
    #[Override]
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
     * @throws TooLong
     * @throws TooShort
     * @throws NotMultipleOf
     */
    #[Override]
    public function reanimate(Identifier $id, ?Timestamp $until = null): void
    {
        $dbId = substr($id->value, 3);

        $selectBuilder = $this->database->createQueryBuilder();
        $result = $selectBuilder
            ->addSelect('concat("db-", id) as id')
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
        $this->republish($job, $until ?? Timestamp::now());
        $this->delete($job);
    }

    /**
     * @throws Exception
     */
    #[Override]
    public function getBuried(Paginate $paginate): Jobs
    {
        $builder = $this->database->createQueryBuilder();
        (new PaginateApplier($paginate))->apply($builder);

        $results = $builder
            ->addSelect('concat("db-", id) as id')
            ->addSelect('name')
            ->addSelect('data')
            ->addSelect('attempt')
            ->from(self::TABLE)
            ->fetchAllAssociative();

        return new Jobs(
            array_map(
                $this->hydrate(...),
                $results,
            ),
            $this->countBuried(),
        );
    }

    /**
     * @param array<mixed> $result
     *
     * @throws NotMultipleOf
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws NotFormat
     * @throws TooLong
     * @throws TooShort
     */
    private function hydrate(array $result): Job
    {
        assert(is_string($result['id']));
        assert(is_string($result['name']));
        assert(is_string($result['data']));
        assert(is_int($result['attempt']));

        $data = unserialize($result['data']);

        if (!$data instanceof DynamicCompositeValueObject) {
            throw new RuntimeException();
        }

        return new Job(
            new Identifier($result['id']),
            new Name($result['name']),
            $data,
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
                        [
                            'x-dead-letter-exchange' => 'delayed',
                            'x-max-priority' => 5,
                        ],
                    ),
                );

            $this->channel->queue_bind(self::QUEUE, self::EXCHANGE);
        }

        return $this->channel;
    }
}

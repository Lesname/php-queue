<?php
declare(strict_types=1);

namespace LesQueue;

use Override;
use LesQueue\Response\Jobs;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Query\QueryBuilder;
use LesValueObject\Number\Exception\NotMultipleOf;
use LesDatabase\Query\Builder\Applier\PaginateApplier;
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
use RuntimeException;
use LesValueObject\Composite\DynamicCompositeValueObject;

final class DbalQueue implements Queue
{
    private const string TABLE = 'queue_job';

    private bool $processing = false;

    public function __construct(private readonly Connection $connection)
    {}

    /**
     * @throws Exception
     */
    #[Override]
    public function publish(Name $name, DynamicCompositeValueObject $data, ?Timestamp $until = null, ?Priority $priority = null): void
    {
        $builder = $this->connection->createQueryBuilder();
        $builder
            ->insert(self::TABLE)
            ->values(
                [
                    'state' => '"ready"',
                    'name' => ':name',
                    'data' => ':data',
                    'until' => ':until',
                    'priority' => ':priority',
                ],
            )
            ->setParameters(
                [
                    'name' => $name,
                    'data' => serialize($data),
                    'until' => $until,
                    'priority' => $priority ?? Priority::normal(),
                ],
            )
            ->executeStatement();
    }

    /**
     * @throws Exception
     */
    #[Override]
    public function republish(Job $job, Timestamp $until, ?Priority $priority = null): void
    {
        $builder = $this->connection->createQueryBuilder();
        $builder
            ->insert(self::TABLE)
            ->values(
                [
                    'state' => '"ready"',
                    'name' => ':name',
                    'data' => ':data',
                    'until' => ':until',
                    'attempt' => ':attempt',
                    'priority' => ':priority',
                ],
            )
            ->setParameters(
                [
                    'name' => $job->name,
                    'data' => serialize($job->data),
                    'until' => $until,
                    'attempt' => $job->attempt->value + 1,
                    'priority' => $priority,
                ],
            )
            ->executeStatement();
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
    public function process(callable $callback): void
    {
        if ($this->processing) {
            throw new RuntimeException('Cannot process when already processing');
        }

        $this->processing = true;

        do {
            $job = $this->findProcessableJob();

            if ($job && $this->markJobReserved($job->id)) {
                $callback($job);
            }

            if ($this->isProcessing() && $job === null) {
                sleep(3);
            }
        } while ($job && $this->isProcessing());

        $this->processing = false;
    }

    #[Override]
    public function isProcessing(): bool
    {
        return $this->processing;
    }

    #[Override]
    public function stopProcessing(): void
    {
        if ($this->isProcessing() === false) {
            throw new RuntimeException();
        }

        $this->processing = false;
    }

    /**
     * @throws Exception
     */
    #[Override]
    public function countProcessing(): int
    {
        $result = $this
            ->connection
            ->createQueryBuilder()
            ->select('count(*)')
            ->from('queue_job')
            ->where('state = "reserved"')
            ->fetchOne();

        if ($result === false) {
            throw new RuntimeException();
        }

        assert((is_string($result) && ctype_digit($result)) || is_int($result));

        return (int)$result;
    }

    /**
     * @throws Exception
     */
    #[Override]
    public function countProcessable(): int
    {
        $builder = $this
            ->connection
            ->createQueryBuilder()
            ->select('count(*)')
            ->from('queue_job');

        $this->applyProcessableWhere($builder);

        $result = $builder
            ->fetchOne();

        if ($result === false) {
            throw new RuntimeException();
        }

        assert((is_string($result) && ctype_digit($result)) || is_int($result));

        return (int)$result;
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

        $builder = $this->connection->createQueryBuilder();
        $builder
            ->delete(self::TABLE)
            ->andWhere('id = :id')
            ->setParameter('id', $id)
            ->executeStatement();
    }

    /**
     * @throws Exception
     */
    #[Override]
    public function bury(Job $job): void
    {
        $builder = $this->connection->createQueryBuilder();
        $builder
            ->update(self::TABLE)
            ->andWhere('id = :id')
            ->setParameter('id', $job->id)
            ->set('state', '"buried"')
            ->set('reserved_on', 'null')
            ->set('reserved_release', 'null')
            ->executeStatement();
    }

    /**
     * @throws Exception
     */
    #[Override]
    public function reanimate(Identifier $id, ?Timestamp $until = null): void
    {
        $builder = $this->connection->createQueryBuilder();
        $builder
            ->update(self::TABLE)
            ->andWhere('id = :id')
            ->setParameter('id', $id)
            ->set('until', ':until')
            ->setParameter('until', $until)
            ->set('state', '"ready"')
            ->set('reserved_on', 'null')
            ->set('reserved_release', 'null')
            ->executeStatement();
    }

    /**
     * @return int<0, max>
     *
     * @throws Exception
     */
    private function countBuried(): int
    {
        $result = $this
            ->connection
            ->createQueryBuilder()
            ->select('count(*)')
            ->from('queue_job')
            ->andWhere('state = "buried"')
            ->fetchOne();

        if ($result === false) {
            throw new RuntimeException();
        }

        if (is_string($result) && ctype_digit($result)) {
            $result = (int)$result;
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
     * @throws NotMultipleOf
     * @throws Exception
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws NotFormat
     * @throws TooLong
     * @throws TooShort
     */
    #[Override]
    public function getBuried(Paginate $paginate): Jobs
    {
        $builder = $this->connection->createQueryBuilder();
        (new PaginateApplier($paginate))->apply($builder);
        $results = $builder
            ->addSelect('id')
            ->addSelect('name as name')
            ->addSelect('data as data')
            ->addSelect('attempt as attempt')
            ->from(self::TABLE)
            ->andWhere('state = "buried"')
            ->addOrderBy('id', 'ASC')
            ->fetchAllAssociative();

        return new Jobs(
            array_map(
                function (array $result): Job {
                    assert(is_string($result['id']) || is_int($result['id']), 'Expected string id');
                    assert(is_string($result['name']), 'Expected string name');
                    assert(is_string($result['attempt']) || is_int($result['attempt']), 'Expected attempt');

                    assert(is_string($result['data']), 'Expected string data');
                    $data = unserialize($result['data']);

                    if (!$data instanceof DynamicCompositeValueObject) {
                        throw new RuntimeException();
                    }

                    return new Job(
                        new Identifier((string)$result['id']),
                        new Name($result['name']),
                        $data,
                        new Unsigned((int)$result['attempt']),
                    );
                },
                $results,
            ),
            $this->countBuried(),
        );
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
    private function findProcessableJob(): ?Job
    {
        $builder = $this->connection->createQueryBuilder();
        $this->applyProcessableWhere($builder);
        $result = $builder
            ->addSelect('id')
            ->addSelect('name as name')
            ->addSelect('data as data')
            ->addSelect('attempt as attempt')
            ->from(self::TABLE)
            ->addOrderBy('priority', 'DESC')
            ->addOrderBy('until', 'ASC')
            ->addOrderBy('id', 'ASC')
            ->setMaxResults(1)
            ->fetchAssociative();

        if (is_array($result)) {
            assert(is_string($result['id']) || is_int($result['id']));
            assert(is_string($result['name']));
            assert(is_string($result['attempt']) || is_int($result['attempt']), 'Expected attempt');

            assert(is_string($result['data']));
            $data = unserialize($result['data']);

            if (!$data instanceof DynamicCompositeValueObject) {
                throw new RuntimeException();
            }

            return new Job(
                new Identifier((string)$result['id']),
                new Name($result['name']),
                $data,
                new Unsigned((int)$result['attempt']),
            );
        }

        return null;
    }

    /**
     * @throws Exception
     */
    private function markJobReserved(Identifier $id): bool
    {
        $builder = $this->connection->createQueryBuilder();
        $this->applyProcessableWhere($builder);
        $updateResult = $builder
            ->update(self::TABLE)
            ->set('reserved_on', 'unix_timestamp()')
            ->set('reserved_release', 'unix_timestamp() + 600')
            ->set('state', '"reserved"')
            ->set('attempt', 'attempt + 1')
            ->andWhere('id = :id')
            ->setParameter('id', $id)
            ->executeStatement();

        return $updateResult === 1;
    }

    private function applyProcessableWhere(QueryBuilder $builder): void
    {
        $where = <<<'SQL'
(
    (
        state = 'ready' 
        AND 
        (
            `until` IS NULL 
            OR 
            `until` < unix_timestamp()
        )
    ) 
    OR
    (
        state = 'reserved'
        AND
        reserved_release < unix_timestamp()
    )
)
SQL;

        $builder->andWhere($where);
    }
}

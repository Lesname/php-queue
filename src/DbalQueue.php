<?php
declare(strict_types=1);

namespace LessQueue;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Query\QueryBuilder;
use LessDatabase\Query\Builder\Applier\PaginateApplier;
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
use RuntimeException;

final class DbalQueue implements Queue
{
    private const TABLE = 'queue_job';

    private bool $processing = false;

    public function __construct(private readonly Connection $connection)
    {}

    /**
     * @param array<mixed> $data
     *
     * @throws Exception
     */
    public function publish(Name $name, array $data, ?Timestamp $until = null): void
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
                ],
            )
            ->setParameters(
                [
                    'name' => $name,
                    'data' => serialize($data),
                    'until' => $until,
                ],
            )
            ->executeStatement();
    }

    /**
     * @throws Exception
     */
    public function republish(Job $job, ?Timestamp $until = null): void
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
                ],
            )
            ->setParameters(
                [
                    'name' => $job->name,
                    'data' => serialize($job->data),
                    'until' => $until,
                    'attempt' => $job->attempt,
                ],
            )
            ->executeStatement();
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

    public function isProcessing(): bool
    {
        return $this->processing;
    }

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
     * @return array<Job>
     *
     * @throws Exception
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws NotFormat
     * @throws PrecisionOutBounds
     * @throws TooLong
     * @throws TooShort
     */
    public function getBuried(Paginate $paginate): array
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

        return array_map(
            function (array $result): Job {
                assert(is_string($result['id']) || is_int($result['id']), 'Expected string id');
                assert(is_string($result['name']), 'Expected string name');
                assert(is_string($result['attempt']) || is_int($result['attempt']), 'Expected attempt');

                assert(is_string($result['data']), 'Expected string data');
                $data = unserialize($result['data']);
                assert(is_array($data), 'Expected unserialized array for data');

                return new Job(
                    new Identifier((string)$result['id']),
                    new Name($result['name']),
                    $data,
                    new Unsigned((int)$result['attempt']),
                );
            },
            $results,
        );
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
            assert(is_array($data));

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

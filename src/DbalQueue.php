<?php
declare(strict_types=1);

namespace LessQueue;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Query\QueryBuilder;
use LessQueue\Job\DbalJob;
use LessQueue\Job\Job;
use LessQueue\Job\Property\Name;
use LessQueue\Job\Property\Priority;
use LessValueObject\Number\Exception\MaxOutBounds;
use LessValueObject\Number\Exception\MinOutBounds;
use LessValueObject\Number\Exception\PrecisionOutBounds;
use LessValueObject\Number\Int\Date\Timestamp;
use LessValueObject\Number\Int\Time\Second;
use LessValueObject\Number\Int\Unsigned;
use LessValueObject\String\Exception\TooLong;
use LessValueObject\String\Exception\TooShort;
use LessValueObject\String\Format\Exception\NotFormat;
use RuntimeException;

final class DbalQueue implements Queue
{
    public function __construct(private readonly Connection $connection)
    {}

    /**
     * @param array<string, mixed> $data
     *
     * @throws Exception
     */
    public function publish(Name $name, array $data, ?Timestamp $until = null, ?Priority $priority = null): void
    {
        $priority ??= Priority::default();

        $builder = $this->connection->createQueryBuilder();
        $builder
            ->insert('queue')
            ->values(
                [
                    'job_state' => '"ready"',
                    'job_name' => ':job_name',
                    'job_data' => ':job_data',
                    'job_priority' => ':job_priority',
                    'job_until' => ':job_until',
                ],
            )
            ->setParameters(
                [
                    'job_name' => $name,
                    'job_data' => serialize($data),
                    'job_priority' => $priority,
                    'job_until' => $until,
                ],
            )
            ->executeStatement();
    }

    /**
     * @throws Exception
     */
    public function republish(Job $job, ?Timestamp $until = null, ?Priority $priority = null): void
    {
        $builder = $this->connection->createQueryBuilder();
        $builder
            ->insert('queue')
            ->values(
                [
                    'job_state' => '"ready"',
                    'job_name' => ':job_name',
                    'job_data' => ':job_data',
                    'job_priority' => ':job_priority',
                    'job_until' => ':job_until',
                    'job_attempt' => ':job_attempt',
                ],
            )
            ->setParameters(
                [
                    'job_name' => $job->getName(),
                    'job_data' => serialize($job->getData()),
                    'job_priority' => ($priority ?? $job->getPriority())->getValue(),
                    'job_until' => $until?->getValue(),
                    'job_attempt' => $job->getAttempt()->getValue(),
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
     * @throws \Exception
     */
    public function reserve(?Second $timeout = null): ?Job
    {
        if ($timeout) {
            $till = time() + $timeout->getValue();
        } else {
            $till = null;
        }

        $job = $this->findProcessableJob();

        while ($job === null && $till !== null && $till < time()) {
            sleep(1);

            $job = $this->findProcessableJob();
        }

        return $job && $this->markJobReserved($job->id)
            ? $job
            : null;
    }

    /**
     * @throws Exception
     */
    public function delete(Job $job): void
    {
        if (!$job instanceof DbalJob) {
            throw new RuntimeException();
        }

        $builder = $this->connection->createQueryBuilder();
        $builder
            ->delete('queue')
            ->andWhere('id = :id')
            ->setParameter('id', $job->id)
            ->executeStatement();
    }

    /**
     * @throws Exception
     */
    public function bury(Job $job): void
    {
        if (!$job instanceof DbalJob) {
            throw new RuntimeException();
        }

        $builder = $this->connection->createQueryBuilder();
        $builder
            ->update('queue')
            ->andWhere('id = :id')
            ->setParameter('id', $job->id)
            ->set('job_state', '"buried"')
            ->set('reserved_on', 'null')
            ->set('reserved_release', 'null')
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
    private function findProcessableJob(): ?DbalJob
    {
        $builder = $this->connection->createQueryBuilder();
        $this->applyProcessableWhere($builder);
        $result = $builder
            ->addSelect('id')
            ->addSelect('job_name as name')
            ->addSelect('job_priority as priority')
            ->addSelect('job_data as data')
            ->addSelect('job_attempt as attempt')
            ->from('queue')
            ->addOrderBy('job_priority', 'DESC')
            ->addOrderBy('job_until', 'ASC')
            ->addOrderBy('id', 'ASC')
            ->setMaxResults(1)
            ->fetchAssociative();

        if (is_array($result)) {
            assert(is_string($result['id']) || is_int($result['id']), 'Expected string id');
            assert(is_string($result['name']), 'Expected string name');
            assert(is_string($result['attempt']) || is_int($result['attempt']), 'Expected attempt');
            assert(is_string($result['priority']) || is_int($result['priority']), 'Expected priority');

            assert(is_string($result['data']), 'Expected string data');
            $data = unserialize($result['data']);
            assert(is_array($data), 'Expected unserialized array for data');

            return new DbalJob(
                (string)$result['id'],
                new Name($result['name']),
                new Priority((int)$result['priority']),
                $data,
                new Unsigned((int)$result['attempt']),
            );
        }

        return null;
    }

    /**
     * @throws Exception
     */
    private function markJobReserved(string $id): bool
    {
        $builder = $this->connection->createQueryBuilder();
        $this->applyProcessableWhere($builder);
        $updateResult = $builder
            ->update('queue')
            ->set('reserved_on', 'unix_timestamp()')
            ->set('reserved_release', 'unix_timestamp() + 600')
            ->set('job_state', '"reserved"')
            ->set('job_attempt', 'job_attempt + 1')
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
        job_state = 'ready' 
        AND 
        (
            job_until IS NULL 
            OR 
            job_until < unix_timestamp()
        )
    ) 
    OR
    (
        job_state = 'reserved'
        AND
        reserved_release < unix_timestamp()
    )
)
SQL;

        $builder->andWhere($where);
    }
}

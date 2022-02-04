<?php
declare(strict_types=1);

namespace LessQueue;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use LessQueue\Job\Job;
use LessQueue\Job\Property\Name;
use LessQueue\Job\Property\Priority;
use LessValueObject\Number\Exception\MaxOutBounds;
use LessValueObject\Number\Exception\MinOutBounds;
use LessValueObject\Number\Exception\PrecisionOutBounds;
use LessValueObject\Number\Int\Date\Timestamp;
use LessValueObject\String\Exception\TooLong;
use LessValueObject\String\Exception\TooShort;
use LessValueObject\String\Format\Exception\NotFormat;

final class DbalQueue implements Queue
{
    private const PROCESSABLE_WHERE = <<<'SQL'
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

    public function __construct(private readonly Connection $connection)
    {}

    /**
     * @param Name $name
     * @param array<string, mixed> $data
     * @param Timestamp|null $until
     * @param Priority|null $priority
     *
     * @throws Exception
     */
    public function put(Name $name, array $data, ?Timestamp $until = null, ?Priority $priority = null): Job
    {
        $builder = $this->connection->createQueryBuilder();

        $priority ??= Priority::default();

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

        $id = $this->connection->lastInsertId();
        assert(is_string($id), 'ID must be a string');

        return new Job($id, $name, $priority, $data);
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
    public function reserve(): ?Job
    {
        $job = $this->findProcessableJob();

        if (!$job instanceof Job) {
            return null;
        }

        return $this->markJobReserved($job)
            ? $job
            : null;
    }

    /**
     * @throws Exception
     */
    public function delete(Job|string $job): void
    {
        $id = $job instanceof Job
            ? $job->id
            : $job;

        $builder = $this->connection->createQueryBuilder();
        $builder
            ->delete('queue')
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
    private function findProcessableJob(): ?Job
    {
        $builder = $this->connection->createQueryBuilder();
        $result = $builder
            ->addSelect('id')
            ->addSelect('job_name as name')
            ->addSelect('job_priority as priority')
            ->addSelect('job_data as data')
            ->from('queue')
            ->andWhere(self::PROCESSABLE_WHERE)
            ->addOrderBy('job_priority', 'DESC')
            ->addOrderBy('job_until', 'ASC')
            ->addOrderBy('id', 'ASC')
            ->setMaxResults(1)
            ->fetchAssociative();

        if (is_array($result)) {
            assert(is_string($result['id']), 'Expected string id');
            assert(is_string($result['name']), 'Expected string name');
            assert(is_string($result['priority']), 'Expected priority');

            assert(is_string($result['data']), 'Expected string data');
            $data = unserialize($result['data']);
            assert(is_array($data), 'Expected unserialized array for data');

            return new Job(
                $result['id'],
                new Name($result['name']),
                new Priority((int)$result['priority']),
                $data,
            );
        }

        return null;
    }

    /**
     * @throws Exception
     */
    private function markJobReserved(Job $job): bool
    {
        $builder = $this->connection->createQueryBuilder();
        $updateResult = $builder
            ->update('queue')
            ->set('reserved_on', 'unix_timestamp()')
            ->set('reserved_release', 'unix_timestamp() + 600')
            ->set('job_state', '"reserved"')
            ->andWhere('id = :id')
            ->andWhere(self::PROCESSABLE_WHERE)
            ->setParameter('id', $job->id)
            ->executeStatement();

        return $updateResult === 1;
    }
}

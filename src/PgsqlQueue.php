<?php

declare(strict_types=1);

namespace LesQueue;

use PDO;
use Override;
use Pdo\Pgsql;
use LesQueue\Job\Job;
use RuntimeException;
use LesQueue\Response\Jobs;
use LesQueue\Job\Property\Name;
use LesQueue\Parameter\Priority;
use LesQueue\Job\Property\Identifier;
use LesValueObject\Composite\Paginate;
use LesValueObject\Number\Int\Unsigned;
use LesValueObject\Number\Int\Date\Timestamp;
use LesValueObject\Composite\DynamicCompositeValueObject;

final class PgsqlQueue extends AbstractQueue
{
    private const string INSERT_QUERY = <<<'SQL'
INSERT INTO queue_job 
    (name, state, data, attempt, until, priority) 
VALUES 
    (:name, :state, :data, :attempt, :until, :priority)
SQL;

    private const string DELETE_QUERY = <<<'SQL'
DELETE FROM queue_job
WHERE id = :id
SQL;

    private const string BURY_QUERY = <<<'SQL'
UPDATE queue_job
SET 
    state = 'buried',
    reserved_on = null,
    reserved_release = null
WHERE id = :id
SQL;

    private const string REANIMATE_QUERY = <<<'SQL'
UPDATE queue_job
SET
    state = 'ready',
    until = :until,
    reserved_on = null,
    reserved_release = null
WHERE id = :id
SQL;

    private const string GET_BURIED_QUERY = <<<'SQL'
SELECT *
FROM queue_job
WHERE state = 'buried'
LIMIT :limit OFFSET :offset
SQL;

    private const string COUNT_BURIED_QUERY = <<<'SQL'
SELECT count(*)
FROM queue_job
WHERE state = 'buried'
SQL;

    private const string FIND_PROCESSABLE_JOB_QUERY = <<<'SQL'
SELECT *
FROM queue_job
WHERE %s
ORDER BY priority DESC, attempt ASC, id asc
LIMIT 1
SQL;

    private const string MARK_JOB_RESERVED_QUERY = <<<'SQL'
UPDATE queue_job
SET
    state = 'reserved',
    reserved_on = :reserved_on,
    reserved_release = :reserved_release,
    reserved_key = :reserved_key,
    attempt = attempt + 1

WHERE id = :id AND %s
SQL;

    private const string WHERE_PROCESSABLE = <<<'SQL'
(
    (
        state = 'ready' 
        AND 
        (
            until IS NULL 
            OR 
            until < extract(epoch from now())
        )
    ) 
    OR
    (
        state = 'reserved'
        AND
        reserved_release < extract(epoch from now())
    )
)
SQL;

    private readonly string $key;

    private int $listen = 60;
    private int $release = 600;

    private bool $processing = false;
    private bool $stopProcessing = false;

    public function __construct(private readonly Pgsql $db)
    {
        $this->key = bin2hex(random_bytes(5));
    }

    /**
     * Max listen is a signed 32bit integer, so 2.147.483 seconds.
     *
     * @see https://www.php.net/manual/en/pdo-pgsql.getnotify.php
     */
    public function setListen(int $listen): void
    {
        if ($listen > 2_147_483) {
            throw new RuntimeException();
        }

        $this->listen = $listen;
    }

    /**
     * Max release is set to 30 days
     */
    public function setRelease(int $release): void
    {
        if ($release > 86_400 * 30) {
            throw new RuntimeException();
        }

        $this->release = $release;
    }

    #[Override]
    protected function insert(Name $name, DynamicCompositeValueObject $data, ?Timestamp $until = null, ?Priority $priority = null, int $attempt = 0): void
    {
        $statement = $this->db->prepare(self::INSERT_QUERY);
        $statement->execute(
            [
                'name' => $name,
                'state' => 'ready',
                'data' => serialize($data),
                'attempt' => $attempt,
                'until' => $until,
                'priority' => ($priority ?? Priority::normal())->value,
            ],
        );
    }

    /**
     * @param callable(Job $job): void $callback
     */
    #[Override]
    public function process(callable $callback): void
    {
        if ($this->processing) {
            throw new RuntimeException('Cannot process when already processing');
        }

        $this->processing = true;
        $start = time();

        try {
            do {
                $job = $this->findProcessableJob();

                if ($job && $this->markJobReserved($job)) {
                    $callback($job);

                    continue;
                }

                $timeout = $this->listen - (time() - $start);

                if ($timeout > 0) {
                    $this->db->exec("LISTEN queue_job_inserted");
                    $result = $this->db->getNotify(timeoutMilliseconds: $timeout * 1_000);

                    if ($result === false) {
                        break;
                    }
                }
            } while (time() - $start < $this->listen && !$this->stopProcessing);
        } finally {
            $this->stopProcessing = false;
            $this->processing = false;
        }
    }

    private function findProcessableJob(): ?Job
    {
        $query = sprintf(
            self::FIND_PROCESSABLE_JOB_QUERY,
            self::WHERE_PROCESSABLE,
        );

        $statement = $this->db->prepare($query);
        $statement->execute();

        $result = $statement->fetch(PDO::FETCH_ASSOC);

        return is_array($result)
            ? $this->getHydrator()($result)
            : null;
    }

    private function markJobReserved(Job $job): bool
    {
        $query = sprintf(
            self::MARK_JOB_RESERVED_QUERY,
            self::WHERE_PROCESSABLE,
        );

        $statement = $this->db->prepare($query);
        $statement->execute(
            [
                'id' => $job->id->value,
                'reserved_on' => time(),
                'reserved_release' => time() + $this->release,
                'reserved_key' => $this->key,
            ],
        );

        return $statement->rowCount() === 1;
    }

    #[Override]
    public function isProcessing(): bool
    {
        return $this->processing;
    }

    #[Override]
    public function stopProcessing(): void
    {
        if ($this->isProcessing()) {
            $this->stopProcessing = true;
        }
    }

    #[Override]
    public function countProcessing(): int
    {
        return $this->isProcessing() ? 1 : 0;
    }

    #[Override]
    public function countProcessable(): int
    {
        $statement = $this->db->prepare(
            sprintf(
                <<<'SQL'
SELECT count(*) 
FROM queue_job 
WHERE %s
SQL,
                self::WHERE_PROCESSABLE
            )
        );
        $statement->execute();

        $result = $statement->fetchColumn();

        if ($result === false) {
            throw new RuntimeException();
        }

        return (int) $result;
    }

    #[Override]
    public function delete(Identifier|Job $item): void
    {
        $id = $item instanceof Job
            ? $item->id->value
            : $item->value;

        $statement = $this->db->prepare(self::DELETE_QUERY);
        $statement->execute(['id' => $id]);
    }

    #[Override]
    public function bury(Job $job): void
    {
        $statement = $this->db->prepare(self::BURY_QUERY);
        $statement->execute(['id' => $job->id->value]);
    }

    #[Override]
    public function reanimate(Identifier $id, ?Timestamp $until = null): void
    {
        $statement = $this->db->prepare(self::REANIMATE_QUERY);
        $statement->execute(
            [
                'id' => $id->value,
                'until' => $until?->value,
            ],
        );
    }

    #[Override]
    public function getBuried(Paginate $paginate): Jobs
    {
        $statement = $this->db->prepare(self::GET_BURIED_QUERY);
        $statement->bindValue('limit', $paginate->perPage->value, PDO::PARAM_INT);
        $statement->bindValue('offset', $paginate->getSkipped(), PDO::PARAM_INT);

        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        $hydrator = $this->getHydrator();
        $jobs = [];

        foreach ($results as $result) {
            assert(is_array($result));

            $jobs[] = $hydrator($result);
        }

        return new Jobs(
            $jobs,
            $this->countBuried(),
        );
    }

    /**
     * @return int<0, max>
     */
    private function countBuried(): int
    {
        $statement = $this->db->prepare(self::COUNT_BURIED_QUERY);
        $statement->execute();

        $result = $statement->fetchColumn();

        if ($result === false) {
            throw new RuntimeException();
        }

        $result = (int) $result;
        assert($result >= 0);

        return $result;
    }

    /**
     * @return callable(array<mixed>): Job
     */
    private function getHydrator(): callable
    {
        return function (array $result): Job {
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
        };
    }
}

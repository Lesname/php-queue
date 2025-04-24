<?php
declare(strict_types=1);

namespace LesQueueTest;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Query\QueryBuilder;
use LesQueue\DbalQueue;
use LesQueue\Job\Job;
use LesQueue\Job\Property\Identifier;
use LesQueue\Job\Property\Name;
use LesQueue\Parameter\Priority;
use LesValueObject\Number\Exception\MaxOutBounds;
use LesValueObject\Number\Exception\MinOutBounds;
use LesValueObject\Number\Int\Date\Timestamp;
use LesValueObject\Number\Int\Unsigned;
use PHPUnit\Framework\TestCase;
use LesValueObject\Number\Exception\NotMultipleOf;
use LesValueObject\Composite\DynamicCompositeValueObject;

/**
 * @covers \LesQueue\DbalQueue
 */
final class DbalQueueTest extends TestCase
{
    /**
     * @throws Exception
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws NotMultipleOf
     * @throws \PHPUnit\Framework\MockObject\Exception
     */
    public function testPut(): void
    {
        $name = new Name('fiz:biz');
        $data = new DynamicCompositeValueObject([]);
        $until = new Timestamp(1);
        $priority = new Priority(4);

        $builder = $this->createMock(QueryBuilder::class);
        $builder
            ->expects(self::once())
            ->method('executeStatement');

        $builder
            ->expects(self::once())
            ->method('insert')
            ->with('queue_job')
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('values')
            ->with(
                [
                    'state' => '"ready"',
                    'name' => ':name',
                    'data' => ':data',
                    'until' => ':until',
                    'priority' => ':priority',
                ],
            )
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('setParameters')
            ->with(
                [
                    'name' => $name,
                    'data' => serialize($data),
                    'until' => $until,
                    'priority' => $priority,
                ]
            )
            ->willReturn($builder);

        $database = $this->createMock(Connection::class);
        $database
            ->expects(self::once())
            ->method('createQueryBuilder')
            ->willReturn($builder);

        $queue = new DbalQueue($database);

        $queue->publish($name, $data, $until, $priority);
    }

    /**
     * @throws Exception
     */
    public function testPutDefault(): void
    {
        $name = new Name('fiz:bar');
        $data = new DynamicCompositeValueObject([]);

        $builder = $this->createMock(QueryBuilder::class);
        $builder
            ->expects(self::once())
            ->method('executeStatement');

        $builder
            ->expects(self::once())
            ->method('insert')
            ->with('queue_job')
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('values')
            ->with(
                [
                    'name' => ':name',
                    'state' => '"ready"',
                    'data' => ':data',
                    'until' => ':until',
                    'priority' => ':priority',
                ],
            )
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('setParameters')
            ->with(
                [
                    'name' => $name,
                    'data' => serialize($data),
                    'until' => null,
                    'priority' => Priority::normal(),
                ],
            )
            ->willReturn($builder);

        $database = $this->createMock(Connection::class);
        $database
            ->expects(self::once())
            ->method('createQueryBuilder')
            ->willReturn($builder);

        $queue = new DbalQueue($database);

        $queue->publish($name, $data);
    }

    /**
     * @throws Exception
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws NotMultipleOf
     * @throws \PHPUnit\Framework\MockObject\Exception
     */
    public function testDeleteViaJob(): void
    {
        $builder = $this->createMock(QueryBuilder::class);
        $builder
            ->expects(self::once())
            ->method('delete')
            ->with('queue_job')
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('executeStatement');

        $builder
            ->expects(self::once())
            ->method('andWhere')
            ->with('id = :id')
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('setParameter')
            ->with('id', '3')
            ->willReturn($builder);

        $connection = $this->createMock(Connection::class);
        $connection
            ->expects(self::once())
            ->method('createQueryBuilder')
            ->willReturn($builder);

        $job = new Job(
            new Identifier('3'),
            new Name('foo:bar'),
            new DynamicCompositeValueObject([]),
            new Unsigned(1),
        );

        $queue = new DbalQueue($connection);
        $queue->delete($job);
    }

    /**
     * @throws Exception
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws NotMultipleOf
     * @throws \PHPUnit\Framework\MockObject\Exception
     */
    public function testBury(): void
    {
        $builder = $this->createMock(QueryBuilder::class);
        $builder
            ->expects(self::once())
            ->method('update')
            ->with('queue_job')
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('executeStatement');

        $builder
            ->method('andWhere')
            ->willReturn($builder);

        $builder
            ->method('set')
            ->willReturn($builder);

        $builder
            ->method('setParameter')
            ->willReturn($builder);

        $connection = $this->createMock(Connection::class);
        $connection
            ->expects(self::once())
            ->method('createQueryBuilder')
            ->willReturn($builder);

        $job = new Job(
            new Identifier('rm-3'),
            new Name('fiz:biz'),
            new DynamicCompositeValueObject([]),
            new Unsigned(2),
        );

        $queue = new DbalQueue($connection);
        $queue->bury($job);
    }
}

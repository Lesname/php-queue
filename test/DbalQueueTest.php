<?php
declare(strict_types=1);

namespace LessQueueTest;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Query\QueryBuilder;
use JsonException;
use LessQueue\DbalQueue;
use LessQueue\Job\Job;
use LessQueue\Job\Property\Name;
use LessQueue\Job\Property\Priority;
use LessValueObject\Number\Int\Date\Timestamp;
use PHPUnit\Framework\TestCase;

/**
 * @covers \LessQueue\DbalQueue
 */
final class DbalQueueTest extends TestCase
{
    /**
     * @throws Exception
     */
    public function testPut(): void
    {
        $name = new Name('fiz:biz');
        $data = [];
        $until = new Timestamp(1);
        $priority = new Priority(3);

        $builder = $this->createMock(QueryBuilder::class);
        $builder
            ->expects(self::once())
            ->method('executeStatement');

        $builder
            ->expects(self::once())
            ->method('insert')
            ->with('queue')
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('values')
            ->with(
                [
                    'job_state' => '"ready"',
                    'job_name' => ':job_name',
                    'job_data' => ':job_data',
                    'job_priority' => ':job_priority',
                    'job_until' => ':job_until',
                ],
            )
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('setParameters')
            ->with(
                [
                    'job_name' => $name,
                    'job_data' => serialize($data),
                    'job_priority' => $priority,
                    'job_until' => $until,
                ]
            )
            ->willReturn($builder);

        $database = $this->createMock(Connection::class);
        $database
            ->expects(self::once())
            ->method('lastInsertId')
            ->willReturn('1');

        $database
            ->expects(self::once())
            ->method('createQueryBuilder')
            ->willReturn($builder);

        $queue = new DbalQueue($database);

        $job = $queue->put($name, $data, $until, $priority);

        self::assertSame('1', $job->id);
        self::assertSame($priority, $job->priority);
        self::assertSame($name, $job->name);
        self::assertSame($data, $job->data);
    }

    /**
     * @throws JsonException
     * @throws Exception
     */
    public function testPutDefault(): void
    {
        $name = new Name('fiz:bar');
        $data = [];

        $builder = $this->createMock(QueryBuilder::class);
        $builder
            ->expects(self::once())
            ->method('executeStatement');

        $builder
            ->expects(self::once())
            ->method('insert')
            ->with('queue')
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('values')
            ->with(
                [
                    'job_name' => ':job_name',
                    'job_state' => '"ready"',
                    'job_data' => ':job_data',
                    'job_priority' => ':job_priority',
                    'job_until' => ':job_until',
                ],
            )
            ->willReturn($builder);

        $builder
            ->expects(self::once())
            ->method('setParameters')
            ->with(
                [
                    'job_name' => $name,
                    'job_data' => serialize($data),
                    'job_priority' => Priority::default(),
                    'job_until' => null,
                ],
            )
            ->willReturn($builder);

        $database = $this->createMock(Connection::class);
        $database
            ->expects(self::once())
            ->method('lastInsertId')
            ->willReturn('1');

        $database
            ->expects(self::once())
            ->method('createQueryBuilder')
            ->willReturn($builder);

        $queue = new DbalQueue($database);

        $job = $queue->put($name, $data);

        self::assertSame('1', $job->id);
        self::assertEquals(Priority::default(), $job->priority);
        self::assertSame($name, $job->name);
        self::assertSame($data, $job->data);
    }

    /**
     * @throws Exception
     */
    public function testReserveNoJob(): void
    {
        $findBuilder = $this->createMock(QueryBuilder::class);
        $findBuilder
            ->method('addSelect')
            ->willReturn($findBuilder);

        $findBuilder
            ->expects(self::once())
            ->method('from')
            ->with('queue')
            ->willReturn($findBuilder);

        $findBuilder
            ->expects(self::once())
            ->method('andWhere')
            ->willReturn($findBuilder);

        $findBuilder
            ->method('addOrderBy')
            ->willReturn($findBuilder);

        $findBuilder
            ->expects(self::once())
            ->method('setMaxResults')
            ->with(1)
            ->willReturn($findBuilder);

        $findBuilder
            ->expects(self::once())
            ->method('fetchAssociative')
            ->willReturn(false);

        $connection = $this->createMock(Connection::class);
        $connection
            ->expects(self::once())
            ->method('createQueryBuilder')
            ->willReturn($findBuilder);

        $queue = new DbalQueue($connection);

        self::assertNull($queue->reserve());
    }

    /**
     * @throws Exception
     */
    public function testReserveJob(): void
    {
        $findBuilder = $this->createMock(QueryBuilder::class);
        $findBuilder
            ->method('addSelect')
            ->willReturn($findBuilder);

        $findBuilder
            ->expects(self::once())
            ->method('from')
            ->with('queue')
            ->willReturn($findBuilder);

        $findBuilder
            ->expects(self::once())
            ->method('andWhere')
            ->willReturn($findBuilder);

        $findBuilder
            ->method('addOrderBy')
            ->willReturn($findBuilder);

        $findBuilder
            ->expects(self::once())
            ->method('setMaxResults')
            ->with(1)
            ->willReturn($findBuilder);

        $findBuilder
            ->expects(self::once())
            ->method('fetchAssociative')
            ->willReturn(
                [
                    'id' => '3',
                    'name' => 'foo:bar',
                    'priority' => '4',
                    'data' => serialize(['fiz' => 'biz']),
                ],
            );

        $reserveBuilder = $this->createMock(QueryBuilder::class);
        $reserveBuilder
            ->expects(self::once())
            ->method('update')
            ->with('queue')
            ->willReturn($reserveBuilder);

        $reserveBuilder
            ->method('set')
            ->willReturn($reserveBuilder);

        $reserveBuilder
            ->expects(self::exactly(2))
            ->method('andWhere')
            ->willReturn($reserveBuilder);

        $reserveBuilder
            ->method('setParameter')
            ->willReturn($reserveBuilder);

        $reserveBuilder
            ->method('executeStatement')
            ->willReturn(1);

        $connection = $this->createMock(Connection::class);
        $connection
            ->expects(self::exactly(2))
            ->method('createQueryBuilder')
            ->willReturnOnConsecutiveCalls($findBuilder, $reserveBuilder);

        $queue = new DbalQueue($connection);
        $job = $queue->reserve();

        self::assertSame('3', $job->id);
        self::assertEquals(new Name('foo:bar'), $job->name);
        self::assertEquals(new Priority(4), $job->priority);
        self::assertSame(['fiz' => 'biz'], $job->data);
    }

    /**
     * @throws Exception
     */
    public function testDeleteViaId(): void
    {
        $builder = $this->createMock(QueryBuilder::class);
        $builder
            ->expects(self::once())
            ->method('delete')
            ->with('queue')
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

        $queue = new DbalQueue($connection);
        $queue->delete('3');
    }

    /**
     * @throws Exception
     */
    public function testDeleteViaJob(): void
    {
        $builder = $this->createMock(QueryBuilder::class);
        $builder
            ->expects(self::once())
            ->method('delete')
            ->with('queue')
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
            '3',
            new Name('foo:bar'),
            Priority::default(),
            [],
        );

        $queue = new DbalQueue($connection);
        $queue->delete($job);
    }

    /**
     * @throws Exception
     */
    public function testBury(): void
    {
        $builder = $this->createMock(QueryBuilder::class);
        $builder
            ->expects(self::once())
            ->method('update')
            ->with('queue')
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
            '3',
            new Name('fiz:biz'),
            Priority::default(),
            [],
        );

        $queue = new DbalQueue($connection);
        $queue->bury($job);
    }
}

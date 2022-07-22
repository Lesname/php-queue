<?php
declare(strict_types=1);

namespace LessQueueTest;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Query\QueryBuilder;
use JsonException;
use LessQueue\DbalQueue;
use LessQueue\Job\DbalJob;
use LessQueue\Job\Property\Name;
use LessQueue\Job\Property\Priority;
use LessValueObject\Number\Exception\MaxOutBounds;
use LessValueObject\Number\Exception\MinOutBounds;
use LessValueObject\Number\Exception\PrecisionOutBounds;
use LessValueObject\Number\Int\Date\Timestamp;
use LessValueObject\Number\Int\Unsigned;
use LessValueObject\String\Exception\TooLong;
use LessValueObject\String\Exception\TooShort;
use LessValueObject\String\Format\Exception\NotFormat;
use PHPUnit\Framework\TestCase;

/**
 * @covers \LessQueue\DbalQueue
 */
final class DbalQueueTest extends TestCase
{
    /**
     * @throws Exception
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws PrecisionOutBounds
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
            ->method('createQueryBuilder')
            ->willReturn($builder);

        $queue = new DbalQueue($database);

        $queue->publish($name, $data);
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
     * @throws MaxOutBounds
     * @throws MinOutBounds
     * @throws NotFormat
     * @throws PrecisionOutBounds
     * @throws TooLong
     * @throws TooShort
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
                    'attempt' => '3',
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
        $queue->reserve();
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

        $job = new DbalJob(
            '3',
            new Name('foo:bar'),
            Priority::default(),
            [],
            new Unsigned(1),
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

        $job = new DbalJob(
            '3',
            new Name('fiz:biz'),
            Priority::default(),
            [],
            new Unsigned(2),
        );

        $queue = new DbalQueue($connection);
        $queue->bury($job);
    }
}

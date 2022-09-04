<?php
declare(strict_types=1);

namespace LessQueueTest;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Query\QueryBuilder;
use LessQueue\DbalQueue;
use LessQueue\Job\Job;
use LessQueue\Job\Property\Identifier;
use LessQueue\Job\Property\Name;
use LessValueObject\Number\Exception\MaxOutBounds;
use LessValueObject\Number\Exception\MinOutBounds;
use LessValueObject\Number\Exception\PrecisionOutBounds;
use LessValueObject\Number\Int\Date\Timestamp;
use LessValueObject\Number\Int\Unsigned;
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
                ]
            )
            ->willReturn($builder);

        $database = $this->createMock(Connection::class);
        $database
            ->expects(self::once())
            ->method('createQueryBuilder')
            ->willReturn($builder);

        $queue = new DbalQueue($database);

        $queue->publish($name, $data, $until);
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
            [],
            new Unsigned(2),
        );

        $queue = new DbalQueue($connection);
        $queue->bury($job);
    }
}

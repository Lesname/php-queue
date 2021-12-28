<?php
declare(strict_types=1);

namespace LessQueueTest\Job;

use LessQueue\Job\Job;
use LessQueue\Job\Property\Name;
use LessQueue\Job\Property\Priority;
use PHPUnit\Framework\TestCase;

/**
 * @covers \LessQueue\Job\Job
 */
final class JobTest extends TestCase
{
    public function testConstruct(): void
    {
        $id = 'fiz';
        $name = new Name('foo:bar');
        $prio = Priority::default();
        $data = ['fiz' => 'biz'];

        $job = new Job($id, $name, $prio, $data);

        self::assertSame($id, $job->id);
        self::assertSame($name, $job->name);
        self::assertSame($prio, $job->priority);
        self::assertSame($data, $job->data);
    }
}

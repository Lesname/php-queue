<?php
declare(strict_types=1);

namespace LessQueueTest\Job\Property;

use LessQueue\Job\Property\Priority;
use PHPUnit\Framework\TestCase;

/**
 * @covers \LessQueue\Job\Property\Priority
 */
final class PriorityTest extends TestCase
{
    public function testNamedConstructs(): void
    {
        self::assertSame(-9, Priority::lowest()->getValue());
        self::assertSame(-6, Priority::lower()->getValue());
        self::assertSame(-3, Priority::low()->getValue());
        self::assertSame(0, Priority::default()->getValue());
        self::assertSame(3, Priority::high()->getValue());
        self::assertSame(6, Priority::higher()->getValue());
        self::assertSame(9, Priority::highest()->getValue());
    }
}

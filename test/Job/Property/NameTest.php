<?php

declare(strict_types=1);

namespace LesQueueTest\Job\Property;

use LesQueue\Job\Property\Name;
use LesValueObject\String\Exception\TooLong;
use LesValueObject\String\Exception\TooShort;
use LesValueObject\String\Format\Exception\NotFormat;
use PHPUnit\Framework\TestCase;

/**
 * @covers \LesQueue\Job\Property\Name
 */
final class NameTest extends TestCase
{
    public function testOk(): void
    {
        $name = new Name('fiz.biZ:bar');

        self::assertSame('fiz.biZ:bar', (string)$name);
        self::assertSame('fiz.biZ:bar', $name->jsonSerialize());
    }

    public function testEmptyName(): void
    {
        $this->expectException(TooShort::class);

        new Name('');
    }

    public function testNameTooLong(): void
    {
        $this->expectException(TooLong::class);

        new Name(str_repeat('a', 51));
    }

    public function testInvalidChar(): void
    {
        $this->expectException(NotFormat::class);

        new Name('#ab');
    }
}

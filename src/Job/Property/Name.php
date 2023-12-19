<?php
declare(strict_types=1);

namespace LessQueue\Job\Property;

use LessValueObject\String\Format\AbstractRegexStringFormatValueObject;

/**
 * @psalm-immutable
 */
final class Name extends AbstractRegexStringFormatValueObject
{
    /**
     * @psalm-pure
     */
    public static function getRegularExpression(): string
    {
        return '/^[a-z][a-zA-Z]*(\.[a-z][a-zA-Z]*)*:[a-z][a-zA-Z]*$/';
    }

    /**
     * @psalm-pure
     */
    public static function getMinimumLength(): int
    {
        return 3;
    }

    /**
     * @psalm-pure
     */
    public static function getMaximumLength(): int
    {
        return 50;
    }
}

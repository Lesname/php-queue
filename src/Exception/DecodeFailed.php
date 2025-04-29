<?php
declare(strict_types=1);

namespace LesQueue\Exception;

use Exception;
use LesQueue\Job\Property\Identifier;

/**
 * @psalm-immutable
 */
final class DecodeFailed extends Exception implements QueueException
{
    public function __construct(public readonly Identifier $id)
    {
        parent::__construct("Failed to decode job with id {$id}.");
    }
}

<?php

declare(strict_types=1);

namespace LesQueue\Config\Factory;

use Pdo\Pgsql;
use SensitiveParameter;
use LesQueue\PgsqlQueue;
use Psr\Container\ContainerInterface;

final class PgsqlQueueFactory
{
    public function __invoke(ContainerInterface $container): PgsqlQueue
    {
        $config = $container->get('config');
        assert(is_array($config));
        assert(is_array($config['queue']));
        assert(is_array($config['queue']['pgsql']));

        return new PgsqlQueue($this->createDB($config['queue']['pgsql']));
    }

    /**
     * @param array<mixed> $config
     */
    private function createDB(#[SensitiveParameter] array $config): Pgsql
    {
        assert(is_string($config['dsn']));

        $username = $config['username'] ?? null;
        assert(is_string($username) || $username === null);

        $password = $config['password'] ?? null;
        assert(is_string($password) || $password === null);

        $options = $config['options'] ?? null;
        assert(is_array($options) || $options === null);

        return new Pgsql($config['dsn'], $username, $password, $options);
    }
}

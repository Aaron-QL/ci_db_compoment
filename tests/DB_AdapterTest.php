<?php
/**
 * Created by PhpStorm.
 * User: akaqin
 * Date: 2019-01-23
 * Time: 15:39
 */
declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use OFashion\DAO\DB_Adapter;
use OFashion\DAO\PDO\PDO_MySQL_Driver;

final class DB_AdapterTest extends TestCase
{
    public function testGetConnection(): void
    {
        $localDir = __DIR__;
        DB_Adapter::loadConfiguration(function () use ($localDir) {
            return require $localDir . '/database.php';
        });

        $this->assertInstanceOf(PDO_MySQL_Driver::class, DB_Adapter::getConnection('db'));
    }
}
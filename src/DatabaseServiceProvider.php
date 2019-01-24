<?php
/**
 * Created by PhpStorm.
 * User: akaqin
 * Date: 2019-01-23
 * Time: 14:46
 */
namespace OFashion\DAO;

use Illuminate\Support\ServiceProvider;

class DatabaseServiceProvider extends ServiceProvider
{
    protected $defer = true;

    public function register()
    {
        $basePath = $this->app->basePath();
        DB_Adapter::loadConfiguration(function () use ($basePath) {
            return require $basePath . '/config/database.php';
        });

        $this->app->bind('db', function () {
            return DB_Adapter::getConnection('db');
        });
    }
}
<?php
# Задача: синхронизировать курьеров из менеджерской бд в основную
# Решение: sql + php
#
#
require_once SYSTEM_ROOT . '/lib/ui.php';
require_once SYSTEM_ROOT . '/lib/DbScript.php';



# подключиться к бд КС2008
# чистый SQL для базы IS
# чистый SQL для базы KS
#
$db     =   CourierexeDbConnection::getDbConnection() or die('Cant connect to db ks2008...');
#

$dbIs   =   new DbScript(Db::instance()->getConnectionLink());
$dbKs   =   new DbScript($db->getConnectionLink());



# 1) получить курьеров из бд КС2008
#
$kurierList =  $dbKs->query("
    SELECT
        NOW()   AS  `transfer_at`
        , `kurier`.`code`
        , `kurier`.`manager`
        , `kurier`.`phonem`
        , `kurier`.`phone`
        , `kurier`.`name`
        , `kurier`.`dateput`
        , `kurier`.`passport`
        , `kurier`.`prava`
        , `cars`.`name`     AS  `cars_name`
        , `cars`.`number`   AS  `cars_number`
    FROM
        `kurier`
            LEFT JOIN  `cars`   ON `kurier`.`car`   =  `cars`.`code`
    WHERE
        `kurier`.`manager` NOT IN (
             1          -- неопределен (нераспределенные курьеры)
            ,20         -- Алексей
            ,1373       -- СГТ
            ,1540       -- ПЕШИЕ
            ,1586       -- НАВИНИЯ
        )
");
#
#
$rows = [];
#
foreach ($kurierList as $k => $r) {
    foreach ($r as &$v)  $v = $dbKs->escape($v);

    $rows[]  =   "SELECT " . implode(', ', $r);
}



# 2) создать таблицу под выгрузку курьеров из KS2008
#
$dbIs->execute("
    CREATE TABLE IF NOT EXISTS `couriers_ks` (
          `transfer_at`     DATETIME
        , `code`            varchar(20)
        , `manager`         int(6)
        , `phonem`          varchar(255)
        , `phone`           varchar(255)
        , `name`            varchar(255)
        , `dateput`         DATE
        , `passport`        varchar(255)
        , `prava`           varchar(255)
        , `cars_name`       varchar(255)
        , `cars_number`     varchar(255)
    )
");
$dbIs->execute("
    TRUNCATE TABLE `couriers_ks`
");

# 3) Заполнить трансферную таблицу актуальным курьерами из KS
#    вставить строки пачками по 1000 шт
# 
$chanks =   array_chunk($rows, 1000);
#
foreach ($chanks as $rows) {
    $dbIs->execute("INSERT INTO `couriers_ks`" . "\n" . implode("\n" . 'UNION ALL' . "\n", $rows));
}




# 4) Группировка курьеров оформлена в KS как привязка к менеджеру (code = manager)
# т.е. это обычная связка id = parent для организации вложенных списков, только поля названы по-другому
# перекладываем группировку в таблицу Хабы (hubs)
#
$dbIs->execute("
    CREATE TEMPORARY TABLE IF NOT EXISTS `hubs_ks`

    SELECT
          `name`
        , `code`
    FROM
        `couriers_ks`
    WHERE
        `code` IN (SELECT DISTINCT `manager`  FROM `couriers_ks`)
");
#
# 4.1) добавить новые хабы (менеджеры в KS)
#
$dbIs->execute("
    INSERT INTO `hubs` (
          `code_in_partner`     -- KS2008.kurier.code
        , `partner_id`
    )
    
    SELECT
        `code`
        , 97                    -- IS.partners.id = 97   (Интернет Логистика)
    FROM
        `hubs_ks`
    WHERE
        `code` NOT IN (
            SELECT
                `code_in_partner`
            FROM
                `hubs`
            WHERE
                `partner_id` = 97
        )
");
#
#
# 4.2) обновить хабы
#
$dbIs->execute("
    UPDATE
        `hubs`
            JOIN `hubs_ks`
            ON  `hubs`.`code_in_partner`    =   `hubs_ks`.`code`
            AND `hubs`.`partner_id`         =   97
    SET
        `hubs`.`name`   =   `hubs_ks`.`name`
");






# 5.1) добавить новых курьеров
#
$dbIs->execute("
    INSERT INTO `couriers` (
          `code_in_partner`
        , `partner_id`
        , `phone`           -- добавить в IS значение поля по-умолчанию ''
        , `created_at`      -- добавить в IS значение поля по-умолчанию NOW()
    )
    
    SELECT
         `couriers_ks`.`code`
        , 97                -- IS.partners.id = 97   (Интернет Логистика)
        , ''
        , NOW()
    FROM
        `couriers_ks`
            LEFT JOIN `couriers`
            ON  `couriers_ks`.`code`    =   `couriers`.`code_in_partner`
            AND `couriers`.`partner_id` =   97
    WHERE
        (LENGTH(`couriers_ks`.`phone`) > 1  OR LENGTH(`couriers_ks`.`phonem`) > 1)
        AND `couriers`.`id` IS NULL
");
#
#
# 5.2) обновить данные курьеров
# затронуть только записи Интернет Логистики: IS.partners.id = 97   
#
$dbIs->execute("
    UPDATE
        `couriers`
            JOIN `couriers_ks`
            ON  `couriers`.`code_in_partner`    =  `couriers_ks`.`code`
            AND `couriers`.`partner_id`         =   97

    SET
          `couriers`.`phone`                =   SUBSTRING(`couriers_ks`.`phonem`, 1, 20)
        , `couriers`.`personal_phone`       =   SUBSTRING(`couriers_ks`.`phone`, 1, 20)
        , `couriers`.`fio`                  =   `couriers_ks`.`name`
        , `couriers`.`locked`               =   IF(`couriers_ks`.`dateput` IS NULL, 0, 1)

        , `couriers`.`courier_document`     =   `couriers_ks`.`passport`
        , `couriers`.`courier_prava`        =   `couriers_ks`.`prava`
        , `couriers`.`courier_car_model`    =   `couriers_ks`.`cars_name`
        , `couriers`.`courier_car_number`   =   `couriers_ks`.`cars_number`

        , `couriers`.`hub_id`               =   (
                                                SELECT
                                                    `id`
                                                FROM
                                                    `hubs`
                                                WHERE
                                                        `code_in_partner`   =   `couriers_ks`.`manager`
                                                    AND `partner_id`        =   97
                                                )

");




die('[DONE:' . time() . ']');

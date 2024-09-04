<?php
# Задача: назначить курьерам ячейки (зоны хранения)
# Решение: sql + php
#
#
require_once SYSTEM_ROOT . '/lib/DbScript.php';

class example
{
  /**
   * Закрепить ячейки (зоны хранения) за курьером на передданую ждд (желаемая дата доставки)
   * @param int $partnerId
   * @param DateTimeImmutable $dateDelivery
   * @return string
   */
  public static function createCourierStorageAreas2(int $partnerId, $dateDelivery)
  {

    $dbIs       =   new DbScript(System::get('Db')->instance()->getConnectionLink());
    $partnerId  =   $dbIs->escape($partnerId);
    $jdd        =   $dbIs->escape($dateDelivery->format('Y-m-d'));


    # 1) проверить первый запуск назначения для переданной ждд
    #
    $first  =   $dbIs->query("
          SELECT
              *
          FROM
              `courier_storage_area_log`
          WHERE
              `date_delivery`  =  $jdd
      ");
    #
    # если в логах нет записей
    # - обнулить назначенные ранее ячейки
    # - добавить запись в лог
    #
    if (count($first) == 0) {
      $dbIs->query("
              UPDATE
                  `couriers`
              SET
                  `area_id` = NULL
              WHERE
                  `partner_id`  =  $partnerId
          ");

      $dbIs->query("
              INSERT INTO `courier_storage_area_log` (
                    `partner_id`
                  , `date_delivery`
                  , `created_at`
              )
              VALUES (
                    $partnerId
                  , $jdd
                  , NOW()
              )
          ");
    }



    # 2) Выбрать список доступных ячеек в базе
    #
    $areas  =   $dbIs->query("
          SELECT
              `id`
          FROM
              `partner_storage_areas`
          WHERE
                  `locked`                =   0       -- актуальные ячейки
              AND `partner_storage_id`    =   319     -- центральный склад
      ");
    $areas  =   array_column($areas, 'id');



    # 3) Выбрать список курьеров из маршрутизации
    #   с назначенными ячейками
    #   на переданную ждд
    #
    $courierAssign  =   $dbIs->query("
          SELECT
                `couriers`.`fio`
              , `couriers`.`id` AS `courier_id`
              , `area_id`
          FROM
              (
                  SELECT DISTINCT
                      `courier_id` AS `id`
                  FROM
                      `courier_orders`
                  WHERE
                      `order_date_delivery`   =   $jdd
              )
              AS `courier_assing`    
                  LEFT JOIN `couriers` USING(`id`)
          ORDER BY
              `couriers`.`id`
      ");



    # 4) отфильтровать
    # - курьеров с назначенными ячейками
    # - занятые ячейки
    # 
    foreach ($courierAssign as $k => $assign) {
      if (empty($assign['area_id']))  continue;

      unset($courierAssign[$k]);

      $key = array_search($assign['area_id'], $areas);
      unset($areas[$key]);
    }
    #
    #
    # 5) назначить ячейку для курьера
    #
    foreach ($courierAssign as $k => $assign) {
      #
      # есть свободные ячейки
      if (!empty($areas)) {
        $areaId = array_shift($areas);
        $courierAssign[$k]['sql']   =   "SELECT {$assign['courier_id']}, $areaId";
      }
      # неосталось свободных ячеек
      else {
        $courierWithoutAreas[] = $assign;
        unset($courierAssign[$k]);
      }
    }



    # 6) DONE: все курьеры получили ячейку и без ячейки никого
    #
    if (empty($courierAssign) && !isset($courierWithoutAreas)) {
      die('OK: Ячейки уже назначены');
      throw new Exception('OK: Ячейки уже назначены');
    }






    # 7) назначить курьерам ячейки
    #   создать временную таблицу для 
    #
    if (!empty($courierAssign)) {
      #
      $dbIs->query("
              CREATE TEMPORARY TABLE `set` (
                  `courier_id`   INT UNSIGNED
                  ,`area_id`      INT UNSIGNED
              )
          ");
      #
      #   заполнить данными
      #
      // ui::vdd($courierAssign);
      $dbIs->query("
              INSERT INTO `set` \n
              " . implode("\n" . "UNION ALL" . "\n", array_column($courierAssign, 'sql')) . "
          ");
      #
      # назначить ячейки
      #
      $dbIs->query("
              UPDATE
                  `couriers`
                      JOIN `set`  ON  `couriers`.`id` = `set`.`courier_id`
              SET
                  `couriers`.`area_id` = `set`.`area_id`
          ");
    }



    # 8) нехватило ячеек
    #
    if (isset($courierWithoutAreas)) {
      die('ERR: Нехватило свободных яцеек (' . count($courierWithoutAreas) . ' шт.)');
      throw new Exception('ERR: Нехватило свободных яцеек (' . count($courierWithoutAreas) . ' шт.)');
    }



    die('OK: Ячейки выданы (' . count($courierAssign) . ' шт.)');
    return true;
  }
}

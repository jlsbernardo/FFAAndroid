<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class Staging extends DB
{

    protected $schemaName = 'analytical';

    public function __construct($country = '', $connection = 'ffa')
    {
        $this->country = $country;
        if ($country != '') {
            $this->stagingTable = 'staging_' . $country['short_name'];
        }

        $this->connection = $connection;

        parent::__construct();
    }

    //delete from staging
    public function deleteStaging()
    {
        $results = $this->__checkRecordStaging();
        
        if (sqlsrv_num_rows($results) > 0) {
            $row = sqlsrv_fetch_array($results);

            if(empty($row['create_on']) && is_null($row['create_on']) || empty($row['update_on']) && is_null($row['update_on'])) {
                $sql = "DELETE FROM [$this->schemaName].[$this->stagingTable] ";
                $this->exec_query($sql);
            } else {
                if (convert_to_datetime($row['create_on'], 'Y-m-d') < date('Y-m-d') || convert_to_datetime($row['update_on'], 'Y-m-d') < date('Y-m-d')) {
                    $sql = "DELETE FROM [$this->schemaName].[$this->stagingTable]";
                    $this->exec_query($sql);
                }
            }
        }
    }

    /**
     * Checking record in staging table
     *
     * @param $data
     * @return object | array
     */
    private function __checkRecordStaging()
    {
        $sql = "SELECT create_on, update_on
        FROM [$this->schemaName].[$this->stagingTable] ORDER BY id DESC";
        $res = $this->exec_query($sql);

        return $res;
    }

}
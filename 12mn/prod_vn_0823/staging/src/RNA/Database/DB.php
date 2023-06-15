<?php

class DB
{
    /**
     * Database name
     *
     * @var string
     */
    public string $database = '';

    /**
     * Database username
     *
     * @var string
     */
    public string $username = 'root';

    /**
     * Database password
     *
     * @var string
     */
    public string $password = '';


    /**
     * Database hostname
     *
     * @var string
     */
    public string $hostname = 'localhost';

    // protected $connection = '';

    /**
     * Open Connection
     *
     * @var
     */
    public $db;

    public function __construct()
    {
        $this->dbconnect();
    }

    // public function __destruct()
    // {
    //     $this->database = '';
    //     $this->username = 'root';
    //     $this->password = '';
    //     $this->hostname = 'localhost';

    //     mysqli_close($this->db);
    // }

    /**
     * Database Connection
     *
     * @return 
     */
    public function dbconnect()
    {
        include dirname(__DIR__, 3).'/config/database.php';

        if ($this->connection == 'analytical') {
            $this->database = $databases['analytical']['database'];
            $this->username = $databases['analytical']['username'];
            $this->password = $databases['analytical']['password'];
            $this->hostname = $databases['analytical']['hostname'];


            $serverName =  $this->hostname; //serverName\instanceName
            $connectionInfo = array( "Database"=>$this->database, "UID" => $this->username, "PWD" => $this->password, 'ReturnDatesAsStrings' => true,  "CharacterSet" => "UTF-8");

            $this->db = sqlsrv_connect($serverName, $connectionInfo);
            
            if (!$this->db) {
                $message = "Connection failed: " . json_encode(sqlsrv_errors(), true)  . "\n" . "PHP Error: " . new Exception();
                echo Logs::error($message);
                exit();
            }
            
        } else {
            foreach ($databases as $key => $database) {
                if ($key == $this->country['short_name'] && $key != 'analytical') {
                    $this->database = $database['ffa_connections']['database'];
                    $this->username = $database['ffa_connections']['username'];
                    $this->password = $database['ffa_connections']['password'];
                    $this->hostname = $database['ffa_connections']['hostname'];
                }
            }
            
            $this->db = new mysqli($this->hostname, $this->username, $this->password, $this->database);
            $this->db->set_charset('utf8');
            
            /**
             * Check Connection
             */
            if (mysqli_connect_errno()) {
                $message = "Connection failed: " . mysqli_connect_error() . "\n" . "PHP Error: " . new Exception();
                echo Logs::error($message);
                exit();
            }
        }
        
        return $this;
    }

    /**
     * Execute Queries
     *
     * @param $sql
     * @return mysqli_query
     */
    public function exec_query($sql)
    {
        if ($this->connection == 'analytical') {
            // echo  date('Y-m-d H:i:s') . ':' . $sql . PHP_EOL;
            $query = sqlsrv_query( $this->db, $sql,  array(), array('Scrollable' => 'buffered'));

            if ($query === false) {
                echo Logs::error("MSSQL Error: " . json_encode(sqlsrv_errors(), true) . "\n" . "Query: " . trim(preg_replace('/\s+/', ' ', trim($sql))) . "\n" . "PHP Error: " . new Exception());
            }
            
            return $query;
            
        } else {
            // echo  date('Y-m-d H:i:s') . ':' . $sql . PHP_EOL;
            $query = $this->db->query($sql);

            if (!mysqli_query($this->db, $sql)) {
                echo Logs::error("Mysql Error: " . $this->db->error . "\n" . "Query: " . trim(preg_replace('/\s+/', ' ', trim($sql))) . "\n" . "PHP Error: " . new Exception());
            }

            return $query;
        }
    }

    public function insert_query($sql)
    {
        $query = $this->db->prepare($sql);
        $query->execute();

        if ($query->affected_rows) {
            return $query;
        }

        if (!mysqli_query($this->db, $sql)) {
            echo Logs::error("Mysql Error: " . $this->db->error . "\n" . "Query: " . trim(preg_replace('/\s+/', ' ', trim($sql))) . "\n" . "PHP Error: " . new Exception());
        }

    }
    
    public function getDeletedFFA($ffaIds)
    {
        $ffaIdsIn = implode(',', $ffaIds);
        $sql = "SELECT ffa_id,deleted_at FROM tbl_deleted_activities WHERE module = '$this->deletedModuleName' AND ffa_id IN ($ffaIdsIn)";
        $result = $this->exec_query($sql);
        $data = [];

        if ($result->num_rows > 0) {
            foreach($result->fetch_all(MYSQLI_ASSOC) as $row) {
                $data [$row['ffa_id']] = [
                    'is_deleted' => date('Y-m-d H:i') >= $row['deleted_at']  ? 1 : 0
                ];
            }
        }
        return $data;
    }

    public function isDeletedFFA($deletedFFAList, $ffaId) {
        if(empty($deletedFFAList)) {
            return false;
        }

        if(isset($deletedFFAList[$ffaId])) {
            return $deletedFFAList[$ffaId]['is_deleted'];
        }

        return false;
    }

    public function getZoneRegionList($territoryIds) {
        $uniqueTerritoryIds = array_unique($territoryIds);
        $territoryIdsIn = implode(',', $uniqueTerritoryIds);
        $sql = "SELECT zone.id AS zone_id, zone.level AS zone_level,region.level AS region_level
                    FROM tbl_area_structure AS zone
                    LEFT JOIN tbl_area_structure AS region
                    ON zone.level = region.id
                    WHERE zone.id IN($territoryIdsIn)";
        $result = $this->exec_query($sql);
        $data = [];
        if ($result->num_rows > 0) {
            $data = $result->fetch_all(MYSQLI_ASSOC);
        }

        return $data;

    }

    public function getZoneRegionSpecific($territoryList, $territoryId) {

        $zoneId = null;
        $regionId = null;

        if(!empty($territoryList)) {
            $key = array_search($territoryId, array_column($territoryList, 'zone_id'));
            if(isset($territoryList[$key])) {
                $zoneId = $territoryList[$key]['zone_level'];
                $regionId = $territoryList[$key]['region_level'];
            }
        }

        return [
            'zone'  => $zoneId,
            'region'    => $regionId
        ];
    }

    /**
     * Checking record in staging table
     *
     * @param $ffaIds
     * @return object | array
     */
    public function getRecordStagingList($ffaIds)
    {
        $ffaIdsIn = implode(',', $ffaIds);
        $sql = "SELECT ffa_id, report_table
                FROM [$this->schemaName].[$this->stagingTable] 
                WHERE report_table = '$this->reportTable' AND ffa_id IN ($ffaIdsIn)";
        $result = $this->exec_query($sql);
        $data = [];
        if (sqlsrv_num_rows($result) > 0) {
            while ($row = sqlsrv_fetch_array($result)) {
                $data[$row['ffa_id']] = $row['ffa_id'];
            }
        }
        return $data;
    }

    public function isInsertedStaging($ffaRecordStagingList, $ffaId) {
        if(empty($ffaRecordStagingList)) {
            return false;
        }

        if(isset($ffaRecordStagingList[$ffaId])) {
            return true;
        }

        return false;        
    }

    public function getRecordStagingListByColumn($ffaIds, $column)
    {
        $ffaIdsIn = implode(',', $ffaIds);
        $sql = "SELECT $column
                FROM [$this->schemaName].[$this->stagingTable] 
                WHERE report_table = '$this->reportTable' AND ffa_id IN ($ffaIdsIn)";
        $result = $this->exec_query($sql);
        $data = [];
        if (sqlsrv_num_rows($result) > 0) {
            while ($row = sqlsrv_fetch_array($result)) {
                $data[$row['ffa_id']] = $row;
            }
        }
        return $data;
    }

    public function getInsertedStaging($ffaRecordStagingList, $ffaId) {
        if(empty($ffaRecordStagingList)) {
            return false;
        }

        if(isset($ffaRecordStagingList[$ffaId])) {
            return $ffaRecordStagingList[$ffaId];
        }

        return false;        
    }

}

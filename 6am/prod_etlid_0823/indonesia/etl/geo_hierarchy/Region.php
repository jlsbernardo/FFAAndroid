<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class Region extends DB
{

    protected $ffaTblName = 'tbl_area_structure';

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

    protected $stagingTable = '';

    protected $reportTable = 'geo_hierarchy_region';

    protected $analyticalTable = 'geo_hierarchy_region';

    protected $country = '';

    protected $connection = '';

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

    public function getStagingETL($last_insert_id='')
    {   
        $sql = "SELECT
            ffa_id,
            level,
            caption
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE
            report_table = '$this->reportTable'
        ORDER BY create_on DESC";

        $result = $this->exec_query($sql);
        
        $regionCount = 0;
        $regionData = [];
        if (sqlsrv_num_rows($result) > 0) {
            while ($row = sqlsrv_fetch_array($result)) {
                $ffa_id = $row['ffa_id'];

                $regionRNAFields = array(
                    'ffa_id'   => $ffa_id,
                    'deleted'  => 0,
                    'caption'  => str_replace("'", "",$row['caption']),
                    'country'  => $this->country['country_name'],
                    'level'    => $row['level']
                );

                $regionData[] = $regionRNAFields;
            }

            //get region data
            $region = $this->__getRegionData($regionData);
            
            return [
                'num_rows'      => $region['count'],
                'last_insert_id'=> $region['last_inserted'],
                'message'       => 'Region records now synced to analytical region table. '
            ];

        } else {

            $message = "No Region Records to sync";
            return [
                'message'       => $message
            ];
        }
    }

    private function __getRegionData($regionData)
    {
        $regionCount = 0;
        $lastInserted = '';
        foreach ($regionData as $data) {
            $strColumns = implode(', ', array_keys($data));
            $strValues =  " '" . implode("', '", array_values($data)) . "' ";
            $ffa_id = $data['ffa_id'];
            // check the record if existing
            $check_record = $this->__checkRegionRecord($data['ffa_id']);

            if (!is_array($check_record)) {
                $regionInsertQuery = "INSERT INTO [$this->schemaName].[$this->analyticalTable] ({$strColumns}) VALUES ({$strValues});";
                $result = $this->exec_query($regionInsertQuery);
                
                if ($result) {
                    $regionCount += 1;
                }
                
                $lastInserted = $ffa_id;

            } else {
                if ($check_record['level'] != $data['level'] || $check_record['caption'] != $data['caption']) {
                    $country = $this->country['country_name'];
                    $regionUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->analyticalTable]
                        SET 
                            caption   = '{$data['caption']}',
                            level     = '{$data['level']}'
                        WHERE [ffa_id] = '$ffa_id'
                        AND country = '$country';";
                    $result = $this->exec_query($regionUpdateQuery);
                    
                    if ($result !== false || $result != -1) {
                        $regionCount += 1;
                    }
                }
            }
        }

        return [
            'count' => $regionCount,
            'last_inserted' => $lastInserted
        ];
    }

    public function updateTblRnaEtlSyncFFA($data)
    {
        $last_insert_id = isset($data['last_insert_id']) ? $data['last_insert_id'] : '';
        $count = isset($data['num_rows']) ? $data['num_rows'] : '';

        $check_record = $this->__checkRecordFFASync();
        $action = !$check_record ? 'create' : 'update';
        $current_timestamp = date('Y-m-d H:i:s');
        if ($last_insert_id) {
            $insert = "INSERT INTO $this->ffaSyncTable (`action_name`, `module`, `last_synced_date`, `status`, `record_count`, `last_insert_id`) VALUES ('{$action}', '$this->reportTable', '{$current_timestamp}', 'done', $count, $last_insert_id);";
            $this->insert_query($insert);
        }
    }

    // Check the user record on ffa.tbl_rna_etl_sync table
    public function __checkRecordFFASync()
    {
        $sql = "SELECT
            id,
            last_insert_id,
            last_synced_date
        FROM
            $this->ffaSyncTable
        WHERE
            module = '$this->reportTable' 

        ORDER BY id DESC
        LIMIT 1";  

        $results = $this->exec_query($sql);

        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }

        return false;
    }

    private function __checkRegionRecord($ffa_id)
    {
        $country = $this->country['country_name'];
        $sql = "SELECT TOP 1 id, level, caption
        FROM [$this->schemaName].[$this->analyticalTable]
        WHERE [ffa_id] = '$ffa_id' AND [country] ='$country'";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }

        return false;
    }
}

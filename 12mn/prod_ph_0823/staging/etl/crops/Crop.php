<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class Crop extends DB
{

    protected $ffaTable = 'tbl_general_crop';

    protected $stagingTable = '';

    protected $reportTable = 'crops';

    protected $country = '';

    protected $connection;

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

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

    public function getDataFromFFA()
    {
        $sql = "SELECT
            id,
            caption
        FROM
            $this->ffaTable";  

        $userInsertQuery = "";
        $result = $this->exec_query($sql);
        $data = [];
        $country = $this->country['country_name'];
        if ($result->num_rows > 0) {
            while ($row = $result->fetch_assoc()) {
                $objVN = new vn_charset_conversion();
                $caption = (strtolower($country)=='vietnam') ? $objVN->convert(str_replace("'", "",$row['caption'])) : str_replace("'", "",$row['caption']);
                $cropsRnaFields = array(
                    'ffa_id'      => $row['id'],
                    'deleted'     => 0,
                    'report_table'=> $this->reportTable,
                    'caption'     => $caption
                );

                $data[] = $cropsRnaFields;
            }

            return [
                'data'  => $data
            ];
        } else {
            $message = "No Crops Records to sync";
            return [
                'data'  => $message
            ];
        }
    }

    public function getStaging()
    {
        $sql = "SELECT
            ffa_id,
            caption
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE
        	report_table = '$this->reportTable' ";

        $userInsertQuery = "";
        $result = $this->exec_query($sql);
        $countCropsAffectedRows = 0;
        $complaintsData = [];
        if (sqlsrv_num_rows($result) > 0) {
            while ($row = sqlsrv_fetch_array($result)) {
                $lastInserted = $row['ffa_id'];

                $cropsRnaFields = array(
                    'ffa_id'      => $row['ffa_id'],
                    'deleted' => 0,
                    'country'     => $this->country['country_name'],
                    'caption'       => $row['caption'],
                );

                $cropsData[] = $cropsRnaFields;
            }

            foreach ($cropsData as $data) {
                $check_record = $this->__checkCropssRecord($lastInserted);

                if (!is_array($check_record)) {
                    $strColumns = implode(', ', array_keys($data));
                    $strValues =  " '" . implode("', '", array_values($data)) . "' ";
                    $userInsertQuery = "INSERT INTO [$this->schemaName].[$this->reportTable] ({$strColumns}) VALUES ({$strValues});";

                    $result = $this->exec_query($userInsertQuery);
                    if ($result) {
                        $countCropsAffectedRows += 1;
                    }
                    
                } else {
                    $country = $this->country['country_name'];
                    $ffaId = $data['ffa_id'];
                    
                    $userUpdateQuery = "
                            UPDATE [$this->schemaName].[$this->reportTable]
                            SET 
                                caption = '{$data['caption']}'
                        WHERE [ffa_id] = '$ffaId'
                        AND country = '$country';";

                    $result =  $this->exec_query($userUpdateQuery);
                    if ($result) {
                        $countCropsAffectedRows += 1;
                    }
                }
            }
            
            return [
                'num_rows'    => $countCropsAffectedRows,
                'last_insert_id'  => $lastInserted
            ];

        } else {
            $message = "No crops Records to sync";
            return $message;
        }
    }

     /**
     * Insert to staging table
     *
     * @param $data
     * @return array
     */
    public function insertIntoStaging($data)
    {
        $count = 0;

        foreach ($data as $cropsRNAFields) {
            $results = $this->__checkRecordStaging($cropsRNAFields);
            if (sqlsrv_num_rows($results) < 1) {
                $strColumns = implode(', ', array_keys($cropsRNAFields));
                $strValues =  " '" . implode("', '", array_values($cropsRNAFields)) . "' ";
                $complaintsInsertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES ({$strValues});";

                $result = $this->exec_query($complaintsInsertQuery);
                if ($result) {
                    $count += 1;
                }
            } else {
                
                $ffaId = $cropsRNAFields['ffa_id'];

                $cropsUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->stagingTable]
                        SET 
                            caption = '{$cropsRNAFields['caption']}'
                            deleted      = 0
                       WHERE [ffa_id] = '$ffaId' AND [report_table] = '$this->reportTable';";

                $result =  $this->exec_query($cropsUpdateQuery);
                if ($result) {
                    $count += 1;
                }
            }
        }

        return [
            'count' => $count
        ];
    }

    /**
     * Checking record in staging table
     *
     * @param $data
     * @return object | array
     */
    private function __checkRecordStaging($data)
    {
        $ffaId = $data['ffa_id'];
        $sql = "SELECT TOP 1 ffa_id, report_table 
            FROM  
            [$this->schemaName].[$this->stagingTable]
            WHERE report_table = '$this->reportTable' AND ffa_id = '$ffaId' ORDER BY id desc";
        $res = $this->exec_query($sql);

        return $res;
    }

        /**
     * Update tbl_rna_etl_sync in FFA Database
     *
     * @param $lastInserted
     * @param $count
     * 
     */
    public function updateRNAEtlSync($lastInserted, $count) 
    {
        $currentDateTime = date('Y-m-d H:i:s');
        $checkRecords = $this->__checkRecordFFASync($lastInserted);

        $action = !$checkRecords ? 'create' : 'update';

        if ($action == 'create') {
            $insert = "INSERT INTO $this->ffaSyncTable (`action_name`, `module`, `last_synced_date`, `status`, `record_count`, `last_insert_id`) 
            VALUES ('create', '$this->reportTable', '$currentDateTime' , 'active', '$count', '$lastInserted');";
            $this->insert_query($insert);
        } else {
            $insert = "INSERT INTO $this->ffaSyncTable (`action_name`, `module`, `last_synced_date`, `status`, `record_count`, `last_insert_id`) 
            VALUES ('update', '$this->reportTable', '$currentDateTime' , 'active', '$count', '$lastInserted');";
            $this->insert_query($insert);
        }
    }

    private function __checkCropssRecord($ffa_id)
    {
        $country = $this->country['country_name'];
        $sql = "SELECT TOP 1 id
        FROM [$this->schemaName].[$this->reportTable]
        WHERE [ffa_id] = '$ffa_id' AND [country] ='$country'";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }

        return false;
    }

    // Check the teams record on ffa.tbl_rna_etl_sync table
    public function __checkRecordFFASync($lastInserted)
    {
        $where = (!is_null($lastInserted) && $lastInserted != '') ? 'AND last_insert_id = ' . $lastInserted : '';
        $sql = "SELECT id, last_insert_id, last_synced_date FROM $this->ffaSyncTable WHERE module = '$this->reportTable' $where LIMIT 1";
        
        $results = $this->exec_query($sql);
        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }
        
        return false;
    }
    
    private function __checkLastRecordFFASync()
    {
        $sql = "SELECT id, last_insert_id, last_synced_date FROM $this->ffaSyncTable WHERE module = '$this->reportTable' AND action_name = 'create' ORDER BY id desc LIMIT 1";
        $results = $this->exec_query($sql);
        
        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }
        
        return false;
    }
}

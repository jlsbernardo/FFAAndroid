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

    protected $limitPerPage = 20000;

    protected $limitChunked = 1000;

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
        //Get total count for chunking 
        $totalCountQuery = $this->getDataFromFFAQuery(1);
        $resultTotalCount = $this->exec_query($totalCountQuery);
        if ($resultTotalCount->num_rows > 0) {
            $resultTotalCountRow = $resultTotalCount->fetch_assoc();
            $totalCount = $resultTotalCountRow['total_count'];
        }
        if($totalCount == 0) {
            $message = "No Crops Records to sync";
            return [
                'data'  => $message,
            ];
        }
            
        $query = $this->getDataFromFFAQuery();
        $queryList = populate_chunked_query_list($query, $totalCount, $this->limitPerPage);
        $userInsertQuery = "";
        $data = [];
        $country = $this->country['country_name'];
        foreach ($queryList as $query) {
            $result = $this->exec_query($query); 
            if ($result->num_rows > 0) {
                $rows = $result->fetch_all(MYSQLI_ASSOC);
                foreach($rows as $row) {
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
            }
        }

        return [
            'data'  => $data
        ];
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
        $dataChunkeds = array_chunk($data, $this->limitChunked);

        foreach ($dataChunkeds as $dataChunked) {
            $ffaIds = array_column($dataChunked, 'ffa_id');
            $ffaRecordStagingList = $this->getRecordStagingList($ffaIds);
            $strColumns = implode(', ', array_keys($dataChunked[0]));
            $insertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES";
            $insertQueryValue = [];

            foreach ($dataChunked as $cropsRNAFields) {
                $isInsertedStaging = $this->isInsertedStaging($ffaRecordStagingList, $cropsRNAFields['ffa_id']);
                if (!$isInsertedStaging) {
                    $strValues =  " '" . implode("', '", array_values($cropsRNAFields)) . "' ";
                    $insertQueryValue[] = "({$strValues})";
                    $count++;  
                } else {
                    $ffaId = $cropsRNAFields['ffa_id'];

                    $cropsUpdateQuery = "
                            UPDATE [$this->schemaName].[$this->stagingTable]
                            SET 
                                caption = '{$cropsRNAFields['caption']}',
                                deleted      = 0
                        WHERE [ffa_id] = '$ffaId' AND [report_table] = '$this->reportTable';";

                    $result =  $this->exec_query($cropsUpdateQuery);
                    if ($result) {
                        $count += 1;
                    }
                }
            }
    
            if(!empty($insertQueryValue)) {
                $insertQuery .= implode(',', $insertQueryValue);
                $result = $this->exec_query($insertQuery);
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

    public function getDataFromFFAQuery($isCount = 0) {
        $select = "SELECT
                    id,
                    caption";

        if($isCount) {
            $select = "SELECT COUNT(*) OVER () AS total_count";
        }

        $sql = "{$select}
                FROM
                $this->ffaTable";

        if($isCount) {
            $sql .= " LIMIT 1";
        }
        return $sql;
    }
}

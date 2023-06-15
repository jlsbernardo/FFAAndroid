<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class Zone extends DB
{
    protected $ffaTable = 'tbl_area_structure';

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

    protected $stagingTable = '';

    protected $analyticalTable = 'geo_hierarchy_zone';

    protected $reportTable = 'geo_hierarchy_zone';

    protected $country = '';

    protected $connection;

    protected $schemaName = 'analytical';

    protected $limitPerPage = 20000;

    protected $limitChunked = 1000;

    protected $stagingColumn = 'ffa_id, report_table, caption, level, node_level';

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
            $message = "No Zone Records to sync";
            return [
                'data'  => $message,
            ];
        }
            
        $query = $this->getDataFromFFAQuery();
        $queryList = populate_chunked_query_list($query, $totalCount, $this->limitPerPage);

        $data = [];
        $country = $this->country['country_name'];
        foreach ($queryList as $query) {
            $result = $this->exec_query($query); 
            if ($result->num_rows > 0) {
                $rows = $result->fetch_all(MYSQLI_ASSOC);
                foreach($rows as $row) {
                    $objVN = new vn_charset_conversion();
                    $caption = (strtolower($country)=='vietnam') ? $objVN->convert( str_replace("'", "",$row['caption'])    ) : str_replace("'", "",$row['caption']);
                    $usersRNAFields = array(
                        'ffa_id'        => $row['id'],
                        'deleted'       => 0,
                        'report_table'  => $this->reportTable,
                        'caption'       => $caption,
                        'level'         => $row['level'],
                        'node_level'    => ($row['node_level']) ? $row['node_level'] : 0,
                    );

                    $data[] = $usersRNAFields;
                }
            }
        }

        return [
            'data' => $data
        ];
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
            $ffaRecordStagingList = $this->getRecordStagingListByColumn($ffaIds, $this->stagingColumn);
            $strColumns = implode(', ', array_keys($dataChunked[0]));
            $insertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES";
            $insertQueryValue = [];

            foreach ($dataChunked as $regionRNAFields) {
                $isInsertedStaging = $this->isInsertedStaging($ffaRecordStagingList, $regionRNAFields['ffa_id']);
                if (!$isInsertedStaging) {
                    $strValues =  " '" . implode("', '", array_values($regionRNAFields)) . "' ";
                    $insertQueryValue[] = "({$strValues})";
                    $count++;
                }  else {
                    $ffaId = $regionRNAFields['ffa_id'];
                    $userUpdateQuery = "
                            UPDATE [$this->schemaName].[$this->stagingTable] 
                            SET 
                                caption= '{$regionRNAFields['caption']}',
                                level = '{$regionRNAFields['level']}',
                                node_level     = '{$regionRNAFields['node_level']}'
                        WHERE [ffa_id] = '$ffaId' AND [report_table] = '$this->reportTable';";
                    // $this->exec_query($userUpdateQuery);

                    $result =  $this->exec_query($userUpdateQuery);
                    if ($result) {
                        $count += sqlsrv_rows_affected($result);
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
        $sql = "SELECT TOP 1 ffa_id, report_table, caption, level, node_level
        FROM [$this->schemaName].[$this->stagingTable] 
        WHERE report_table = '$this->reportTable' AND ffa_id = '$ffaId' 
        ORDER BY id desc";
        
        $res = $this->exec_query($sql);

        if (sqlsrv_num_rows($res) > 0) {
            return sqlsrv_fetch_array($res);
        }

        return $res;
    }

    public function getDataFromFFAQuery($isCount = 0) {
        $select = "SELECT
                    id,
                    caption,
                    level,
                    node_level";

        if($isCount) {
            $select = "SELECT COUNT(*) OVER () AS total_count";
        }

        $sql = "{$select}
                FROM
                    $this->ffaTable
                WHERE
                    node_level = 1";

        if($isCount) {
            $sql .= " LIMIT 1";
        }
        return $sql;
    }
    
}

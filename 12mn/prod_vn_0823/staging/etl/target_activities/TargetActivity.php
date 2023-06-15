<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class TargetActivity extends DB
{

    protected $ffaTable = 'tbl_target_activities';

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

    protected $stagingTable = '';

    protected $analyticalTable = 'target_activities';

    protected $reportTable = 'target_activities';

    protected $country = '';

    protected $connection = '';

    protected $last_insert_id;

    protected $schemaName = 'analytical';

    protected $limitPerPage = 20000;

    protected $limitChunked = 1000;

    public function __construct($country = '', $connection = 'ffa', $last_insert_id = '')
    {
        $this->country = $country;
        if ($country != '') {
            $this->stagingTable = 'staging_' . $country['short_name'];
        }

        $this->connection = $connection;
        $this->last_insert_id = $last_insert_id;

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
            $message = "No Target Activity Records to sync";
            return [
                'data'  => $message,
            ];
        }
            
        $query = $this->getDataFromFFAQuery();
        $queryList = populate_chunked_query_list($query, $totalCount, $this->limitPerPage);
        $targetActivityInsertQuery = "";
        $data = [];
        $country = $this->country['country_name'];
        foreach ($queryList as $query) {
            $result = $this->exec_query($query); 
            if ($result->num_rows > 0) {
                $rows = $result->fetch_all(MYSQLI_ASSOC);
                foreach($rows as $row) {
                    $updated_at = $row['updated_at'] != "0000-00-00 00:00:00" ? strtotime($row['updated_at']) : 0;
                    date_default_timezone_set("Asia/Ho_Chi_Minh");
                    $objVN = new vn_charset_conversion();
                    $key = (strtolower($country)=='vietnam') ? $objVN->convert(str_replace("'", "",$row['key'])) : str_replace("'", "",$row['key']);
                    $targetActivityRNAFields = array(
                        'ffa_id'        => $row['portal_setting_id'],
                        'deleted'       => 0,
                        'report_table'  => $this->reportTable,
                        'module'        => $row['module'],
                        '[month]'       => date("M-Y"),
                        '[type]'        => $row['type'],
                        '[key]'         => $key,
                        '[value]'       => $row['value'],
                        '[create_on]'   => strtotime($row['created_at']),
                        '[created_by]'  => $row['created_by'],
                        '[update_on]'   => $updated_at,
                        '[update_by]'   => $row['updated_by'],
                        '[activity_type]' => $row['activity_type']
                    );
                    $data[] = $targetActivityRNAFields;
                }
            }
        } 
    
        return [
            'data'  => $data
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
        $country = $this->country['country_name'];
        $dataChunkeds = array_chunk($data, $this->limitChunked);

        foreach ($dataChunkeds as $dataChunked) {
            $ffaIds = array_column($dataChunked, 'ffa_id');
            $ffaRecordStagingList = $this->getRecordStagingList($ffaIds);
            $strColumns = implode(', ', array_keys($dataChunked[0]));
            $insertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES";
            $insertQueryValue = [];

            foreach ($dataChunked as $targetActivityRNAFields) {
                $isInsertedStaging = $this->isInsertedStaging($ffaRecordStagingList, $targetActivityRNAFields['ffa_id']);
                if (!$isInsertedStaging) {
                    $strValues =  " '" . implode("', '", array_values($targetActivityRNAFields)) . "' ";
                    $insertQueryValue[] = "({$strValues})";
                    $count++;

                } else {
                    $ffaId = $targetActivityRNAFields['ffa_id'];
                    $objVN = new vn_charset_conversion();
                    $key = (strtolower($country)=='vietnam') ? $objVN->convert(str_replace("'", "",$targetActivityRNAFields['[key]'])) : str_replace("'", "",$targetActivityRNAFields['[key]']);
                    $created_on = $targetActivityRNAFields['[create_on]'] != 0 ? convert_to_datetime($targetActivityRNAFields['[create_on]']) : NULL;
                    $updated_on = $targetActivityRNAFields['[update_on]'] != 0 ? convert_to_datetime($targetActivityRNAFields['[update_on]']) : NULL;
                    $country = $this->country['country_name'];

                    $updateQuery = "
                            UPDATE [$this->schemaName].[$this->stagingTable] 
                            SET 
                                [module]= '{$targetActivityRNAFields['module']}',
                                [type] = '{$targetActivityRNAFields['[type]']}',
                                [key]     = '{$key}',
                                [value]     = '{$targetActivityRNAFields['[value]']}',
                                [create_on] = '{$targetActivityRNAFields['[create_on]']}',
                                [created_by] = '{$targetActivityRNAFields['[created_by]']}',
                                [update_on] = '{$targetActivityRNAFields['[update_on]']}',
                                [update_by] = '{$targetActivityRNAFields['[update_by]']}',
                                [activity_type] = '{$targetActivityRNAFields['[activity_type]']}'
                        WHERE [ffa_id] = '$ffaId' AND [report_table] = '$this->reportTable';";

                    $result =  $this->exec_query($updateQuery);

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
        $month = date("M-Y");
        $sql = "SELECT  TOP 1 ffa_id, report_table FROM [$this->schemaName].[$this->stagingTable] 
        WHERE report_table = '$this->reportTable' 
        AND ffa_id = '$ffaId' AND [month] = '$month' ORDER BY id DESC";
        $res = $this->exec_query($sql);

        if (sqlsrv_num_rows($res) > 0) {
            return sqlsrv_fetch_array($res);
        }

        return false;
    }


    public function getDataFromFFAQuery($isCount = 0) {
        $select = "SELECT
                    id,
                    portal_setting_id,
                    module,
                    `type`,
                    `key`,
                    `value`,
                    CONVERT_TZ(created_at, '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."') as created_at,
                    `created_by`,
                    CONVERT_TZ(updated_at, '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."') as updated_at,
                    `updated_by`,
                    `activity_type`";

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

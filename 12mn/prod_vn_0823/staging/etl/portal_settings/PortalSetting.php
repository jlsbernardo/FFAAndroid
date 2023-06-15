<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class PortalSetting extends DB
{

    protected $ffaTable = 'defaultvalues';

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

    protected $stagingTable = '';

    protected $analyticalTable = 'portal_settings';

    protected $reportTable = 'portal_settings';

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
            $message = "No Portal Setting Records to sync";
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
                    $usersRNAFields = array(
                        'ffa_id' => $row['id'],
                        'deleted' => 0,
                        'report_table'  => $this->reportTable,
                        'module'    => $row['module'],
                        '[type]'    => $row['type'],
                        '[key]'    => str_replace("'", "", $row['key']),
                        '[value]' => str_replace("'", "", $row['value']),
                        '[update_by]' => $row['updated_by'],
                        '[update_on]' => strtotime($row['updated_on'])
                    );
                    $data[] = $usersRNAFields;
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
        $dataChunkeds = array_chunk($data, $this->limitChunked);

        foreach ($dataChunkeds as $dataChunked) {
            $ffaIds = array_column($dataChunked, 'ffa_id');
            $ffaRecordStagingList = $this->getRecordStagingList($ffaIds);
            $strColumns = implode(', ', array_keys($dataChunked[0]));
            $insertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES";
            $insertQueryValue = [];

            foreach ($dataChunked as $portalSettingRNAFields) {
                $isInsertedStaging = $this->isInsertedStaging($ffaRecordStagingList, $portalSettingRNAFields['ffa_id']);
                if (!$isInsertedStaging) {
                    $strValues =  " '" . implode("', '", array_values($portalSettingRNAFields)) . "' ";
                    $insertQueryValue[] = "({$strValues})";
                    $count++;  

                } else {
                    $ffaId = $portalSettingRNAFields['ffa_id'];
                    $updated_at = $portalSettingRNAFields['[update_on]'] != 0 ? convert_to_datetime($portalSettingRNAFields['[update_on]']) : NULL;

                    $portalSettingUpdateQuery = "
                            UPDATE [$this->schemaName].[$this->stagingTable] 
                            SET 
                                [module]= '{$portalSettingRNAFields['module']}',
                                [type] = '{$portalSettingRNAFields['[type]']}',
                                [key]     = '{$portalSettingRNAFields['[key]']}',
                                [value]     = '{$portalSettingRNAFields['[value]']}',
                                [update_on]  = '{$portalSettingRNAFields['[update_on]']}',
                                [update_by]  = '{$portalSettingRNAFields['[update_by]']}'
                        WHERE [ffa_id] = '$ffaId' AND [report_table] = '$this->reportTable'";

                    $result =  $this->exec_query($portalSettingUpdateQuery);

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
        $sql = "SELECT  TOP 1 ffa_id, report_table FROM [$this->schemaName].[$this->stagingTable] 
        WHERE report_table = '$this->reportTable' 
        AND ffa_id = '$ffaId' ORDER BY id DESC";
        $res = $this->exec_query($sql);

        if (sqlsrv_num_rows($res) > 0) {
            return sqlsrv_fetch_array($res);
        }

        return false;
    }

    public function getDataFromFFAQuery($isCount = 0) {

        $select = "SELECT
                    id,
                    module,
                    `type`,
                    `key`,
                    `value`,
                    `updated_on`,
                    `updated_by`";

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

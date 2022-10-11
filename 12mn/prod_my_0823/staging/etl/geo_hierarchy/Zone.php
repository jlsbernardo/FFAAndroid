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
            caption,
            level,
            node_level 
        FROM
            $this->ffaTable
        WHERE
            node_level = 1
        ";

        $result = $this->exec_query($sql);
        $data = [];
        $country = $this->country['country_name'];
        if ($result->num_rows > 0) {
            while ($row = $result->fetch_assoc()) {
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

            return [
                'data' => $data
            ];
        } else {
            $message = "No Zone Records to sync";

            return [
                'data' => $message
            ];
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

        foreach ($data as $regionRNAFields) {
            $results = $this->__checkRecordStaging($regionRNAFields);

            if (!is_array($results)) {
                $strColumns = implode(', ', array_keys($regionRNAFields));
                $strValues =  " '" . implode("', '", array_values($regionRNAFields)) . "' ";
                $userInsertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES ({$strValues});";

                $result = $this->exec_query($userInsertQuery);
                if ($result) {
                    $count += 1;
                }

            } else {

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
    
}

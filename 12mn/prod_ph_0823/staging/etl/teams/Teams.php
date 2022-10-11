<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class Teams extends DB
{

    protected $ffaTable = 'defaultvalues';

    protected $stagingTable = '';

    protected $analyticalTable = 'teams';

    protected $reportTable = 'teams';

    protected $country = '';

    protected $connection = '';

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
            module,
            `type`,
            `key`,
            `value`
        FROM
            $this->ffaTable
        WHERE `key` = 'user-team-list'";

        $userInsertQuery = "";
        $result = $this->exec_query($sql);
        $data = [];
        $country = $this->country['country_name'];
        if ($result->num_rows > 0) {
            while ($row = $result->fetch_assoc()) {
                $objVN = new vn_charset_conversion();
                $teams = (strtolower($country)=='vietnam') ? $objVN->convert(    $row['value']    ) : $row['value'];
                $teamsRNAFields = array(
                    'ffa_id'        => $row['id'],
                    'deleted'       => 0,
                    'report_table'  => $this->reportTable,
                    'module'        => $row['module'],
                    '[type]'        => $row['type'],
                    '[key]'         => $row['key'],
                    '[value]'       => $teams,
                );

                $data[] = $teamsRNAFields;
            }

            return [
                'data'  => $data
            ];
        } else {
            $message = "No Portal Setting Records to sync";
            return [
                'data'  => $message
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

        foreach ($data as $userRNAFields) {
            $results = $this->__checkRecordStaging($userRNAFields);

            if (sqlsrv_num_rows($results) < 1) {
                $strColumns = implode(', ', array_keys($userRNAFields));
                $strValues =  " '" . implode("', '", array_values($userRNAFields)) . "' ";
                $userInsertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES ({$strValues});";

                $result = $this->exec_query($userInsertQuery);
                $count += 1;
            } else {
                print_r('update');die;
                $ffaId = $userRNAFields['ffa_id'];
                
                $userUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->stagingTable]
                        SET 
                            [type]      = '{$userRNAFields['`type`']}',
                            [key]       = '{$userRNAFields['`key`']}',
                            [value]     = '{$userRNAFields['`value`']}',
                            [deleted]   = 0
                       WHERE [ffa_id]   = '$ffaId' AND 
                       [report_table]   = '$this->reportTable';";
                // $this->exec_query($userUpdateQuery);

                $result =  $this->exec_query($userUpdateQuery);
                $count += sqlsrv_rows_affected($result);
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
        $sql = "SELECT TOP 1 ffa_id, report_table FROM [$this->schemaName].[$this->stagingTable] WHERE report_table = '$this->reportTable' AND ffa_id = '$ffaId' ORDER BY id desc";
        $res = $this->exec_query($sql);

        return $res;
    }
    
}

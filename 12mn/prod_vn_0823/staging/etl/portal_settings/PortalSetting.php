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
        $sql = "SELECT
            id,
            module,
            `type`,
            `key`,
            `value`,
            `updated_on`,
            `updated_by`
        FROM
            $this->ffaTable
        ";

        $userInsertQuery = "";
        $result = $this->exec_query($sql);
        $data = [];
        $country = $this->country['country_name'];
        if ($result->num_rows > 0) {
            while ($row = $result->fetch_assoc()) {
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

            return [
                'data'  => $data
            ];
        } else {
            $message = "No Portal Setting Records to sync";

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

        foreach ($data as $portalSettingRNAFields) {
            $results = $this->__checkRecordStaging($portalSettingRNAFields);

            if (!is_array($results)) {
                $strColumns = implode(', ', array_keys($portalSettingRNAFields));
                $strValues =  " '" . implode("', '", array_values($portalSettingRNAFields)) . "' ";
                $portalSettingInsertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES ({$strValues});";

                $result = $this->exec_query($portalSettingInsertQuery);
                
                if ($result) {
                    $count += 1;
                }

            } else {
                $ffaId = $portalSettingRNAFields['ffa_id'];
                $updated_at = $row['update_on'] != 0 ? convert_to_datetime($row['update_on']) : NULL;
                $country = $this->country['country_name'];

                $portalSettingUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->stagingTable] 
                        SET 
                            [module]= '{$portalSettingRNAFields['module']}',
                            [type] = '{$portalSettingRNAFields['[type]']}',
                            [key]     = '{$portalSettingRNAFields['[key]']}',
                            [value]     = '{$portalSettingRNAFields['[value]']}',
                            [update_on]  = '{$updated_at}',
                            [update_by]  = '{$portalSettingRNAFields['update_by']}'
                       WHERE [ffa_id] = '$ffaId' AND [report_table] = '$this->reportTable' AND [country] = '$country';";

                $result =  $this->exec_query($portalSettingUpdateQuery);

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
        $sql = "SELECT  TOP 1 ffa_id, report_table FROM [$this->schemaName].[$this->stagingTable] 
        WHERE report_table = '$this->reportTable' 
        AND ffa_id = '$ffaId' ORDER BY id DESC";
        $res = $this->exec_query($sql);

        if (sqlsrv_num_rows($res) > 0) {
            return sqlsrv_fetch_array($res);
        }

        return false;
    }

}

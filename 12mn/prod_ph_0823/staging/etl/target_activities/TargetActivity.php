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
            portal_setting_id,
            module,
            `type`,
            `key`,
            `value`,
            CONVERT_TZ(created_at, '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."') as created_at,
            `created_by`,
            CONVERT_TZ(updated_at, '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."') as updated_at,
            `updated_by`,
            `activity_type`
        FROM
            $this->ffaTable
        ";

        $targetActivityInsertQuery = "";
        $result = $this->exec_query($sql);
        $data = [];
        $country = $this->country['country_name'];
        if ($result->num_rows > 0) {
            while ($row = $result->fetch_assoc()) {
                $updated_at = $row['updated_at'] != "0000-00-00 00:00:00" ? strtotime($row['updated_at']) : 0;
                date_default_timezone_set("Asia/Manila");
                $targetActivityRNAFields = array(
                    'ffa_id'        => $row['portal_setting_id'],
                    'deleted'       => 0,
                    'report_table'  => $this->reportTable,
                    'module'        => $row['module'],
                    '[month]'       => date("M-Y"),
                    '[type]'        => $row['type'],
                    '[key]'         => str_replace("'", "",$row['key']),
                    '[value]'       => $row['value'],
                    '[create_on]'   => strtotime($row['created_at']),
                    '[created_by]'  => $row['created_by'],
                    '[update_on]'   => $updated_at,
                    '[update_by]'   => $row['updated_by'],
                    '[activity_type]' => $row['activity_type']
                );
                $data[] = $targetActivityRNAFields;
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
                $created_on = $row['create_on'] != 0 ? convert_to_datetime($row['create_on']) : NULL;
                $updated_on = $row['update_on'] != 0 ? convert_to_datetime($row['update_on']) : NULL;
                $country = $this->country['country_name'];

                $portalSettingUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->stagingTable] 
                        SET 
                            [module]= '{$portalSettingRNAFields['module']}',
                            [type] = '{$portalSettingRNAFields['[type]']}',
                            [key]     = '{$portalSettingRNAFields['[key]']}',
                            [value]     = '{$portalSettingRNAFields['[value]']}',
                            [create_on] = '{$created_on}',
                            [create_by] = '{$portalSettingRNAFields['created_by']}',
                            [update_on] = '{$updated_on}',
                            [update_by] = '{$portalSettingRNAFields['update_by']}',
                            [activity_type] = '{$portalSettingRNAFields['activity_type']}'
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

}


<?php

require_once (dirname(__FILE__, 3). '/src/RNA/Database/DB.php');

/**
 * This script is for generating .sql and .txt file for staging (for all countries)
 * That will be imported to Analytical Database using any database tools ie. MySQL Workbench
 * 
 * @return \Logs
 */

class Demo extends DB
{
    protected $ffaTable = 'tbl_ffa_demo';

    protected $stagingTable = '';

    protected $reportTable = 'demo_reports';

    protected $ffaGps = 'tbl_ffa_images';

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

        //call DB contructor
        parent::__construct();
    }

    private function __checkDeleted($ffa_id)
    {
        echo Logs::success("12MN ID Demo __checkDeleted Process Starts: " . date('Y-m-d H:i:s') . "\n");
        $sql = "SELECT ffa_id FROM tbl_deleted_activities WHERE module = '$this->reportTable' AND 'ffa_id' = $ffa_id limit 1";
        $result = $this->exec_query($sql);

        if ($result->num_rows > 0) {
            // return true;
            $row = $result->fetch_assoc();
            if (date('Y-m-d H:i') >= $row['deleted_at']) {
                return true;
            }
        }
        echo Logs::success("12MN ID Demo __checkDeleted Process End: " . date('Y-m-d H:i:s') . "\n");
        return false;
    }

    /**
     * Get Data from FFA Database
     *
     * @return string | array
     */
    public function getDataFromFFA()
    {
        echo Logs::success("12MN ID Demo getDataFromFFA Process Starts: " . date('Y-m-d H:i:s') . "\n");

        $checkLastRecord = $this->__checkLastRecordFFASync();
        $lastInserted = ($checkLastRecord) ? $checkLastRecord['last_insert_id'] : null;
        $only2022_data = strtotime('2022-04-01 00:00:00');

        $sql = "SELECT
            $this->ffaTable.id,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME($this->ffaTable.create_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as create_on,
            created_by,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME(update_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as update_on,
            update_by,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME(plan_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as plan_on,
            plan_by,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME(approved_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as approved_on,
            approved_by,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME(closed_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as closed_on,
            closed_by,
            execute_on,
            execute_by,
            date,
            followup,
            team,
            host_name,
            host_phone,
            month,
            $this->ffaTable.status,
            temp_followup,
            temp_execute,
            products,
            territory,
            supervisore,
            supervisorf,
            marked_by_e,
            marked_by_f,
            marked_on_e,
            marked_on_f,
            crop,
            tmp.lat as imglat,
            tmp.lng as imglng
        FROM
            $this->ffaTable
        LEFT JOIN
        (
            SELECT s.* FROM $this->ffaGps AS s ORDER BY s.id DESC
        ) AS tmp ON $this->ffaTable.id = tmp.ref_id
        AND tmp.category='demo' AND (tmp.sub_category='before' OR tmp.sub_category='after')
        WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME($this->ffaTable.create_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."'))>=$only2022_data
        GROUP BY 
            $this->ffaTable.id           
        order by
            $this->ffaTable.create_on
        desc";

        $data = [];
        $result = $this->exec_query($sql);
        $countDemoAffectedRows = 0;
        $country = $this->country['country_name'];
        if ($result->num_rows > 0) {
            while ($row = $result->fetch_assoc()) {

                $deleted = $this->__checkDeleted($row['id']);
                
                $territory = $row['territory'];

                $zoneRegion = $this->getDemoZoneRegion($territory);
                if($row['closed_by']!==null && $row['closed_by']==$row['created_by']){
                    $supervisorId = $territory!==null ? $this->getSupervisor($row['id'],intval($territory),$row['team']) : null;
                }else{
                    $supervisorId = $row['created_by'];
                }

                $temp_execute_decode = json_decode($row['temp_execute'], true);
                $temp_followup_decode = json_decode($row['temp_followup'], true);
                
                $temp_execute = (isset($temp_execute_decode) && $temp_execute_decode['client']['time']) ? $temp_execute_decode['client']['time'] : null;
                $temp_followup = (isset($temp_followup_decode) && $temp_followup_decode['client']['time']) ? $temp_followup_decode['client']['time'] : null;

                $objVN = new vn_charset_conversion();
                $hn = preg_replace("/[^a-zA-Z0-9]+/", "", html_entity_decode(str_replace("'", "", $row['host_name']), ENT_QUOTES));
                $hostname = (strtolower($country)=='vietnam') ? $objVN->convert($hn) : $hn;
                $team = (strtolower($country)=='vietnam') ? $objVN->convert($row['team']) : $row['team'];
                $products = (strtolower($country)=='vietnam') ? $objVN->convert($row['products']) : $row['products'];

                $demoRNAFields = array(
                    'ffa_id'        => $row['id'],
                    'deleted'       => $deleted ? '1' : '0',
                    'report_table'  => $this->reportTable,
                    'create_on'     => ($row['create_on']) ? $row['create_on'] : null,
                    // 'created_by'    => $row['created_by'],
                    'created_by'    => $supervisorId,
                    'update_by'     => $row['update_by'],
                    'plan_by'       => $row['plan_by'],
                    'approved_by'   => $row['approved_by'],
                    'closed_by'     => $row['closed_by'],
                    'execute_by'    => $row['execute_by'],
                    'team'          => (strtolower($country)=='thailand') ? $team : trim($team),
                    'host_name'     => $hostname,
                    'host_phone'    => $row['host_phone'],
                    'month'         => $row['month'],
                    'status'        => $row['status'],
                    'temp_followup' => $temp_execute,
                    'temp_execute'  => $temp_followup,
                    'products'      => $products,
                    'territory'     => $row['territory'],
                    'zone'          => $zoneRegion['zone'],
                    'region'        => $zoneRegion['region'],
                    'supervisorf'   => $row['supervisorf'],
                    'supervisore'   => $row['supervisore'],
                    'marked_by_e'   => !is_null($row['marked_by_e']) ? $row['marked_by_e'] : 0,
                    'marked_by_f'   => !is_null(($row['marked_by_f'])) ? $row['marked_by_f'] : 0,
                    'crop_id'       => ($row['crop']) ? $row['crop'] : 0,
                    'lat'           => ($row['imglat']) ?$row['imglat']:0,
                    'lng'           => ($row['imglng']) ? $row['imglng']:0
                );

                if (!empty($row['update_on']) && !is_null($row['update_on'])) {
                    $demoRNAFields['update_on'] = $row['update_on'];
                }

                if (!empty($row['closed_on']) && !is_null($row['closed_on'])) {
                    $demoRNAFields['closed_on'] = $row['closed_on'];
                }
                
                if (!empty($row['approved_on']) && !is_null($row['approved_on'])) {
                    $demoRNAFields['approved_on'] = $row['approved_on'];
                }

                if (!empty($row['execute_on']) && !is_null($row['execute_on'])) {
                    $demoRNAFields['execute_on'] = $row['execute_on'];
                }

                if (!empty($row['plan_on']) && !is_null($row['plan_on'])) {
                    $demoRNAFields['plan_on'] = $row['plan_on'];
                }

                if (!empty($row['date']) && !is_null($row['date'])) {
                    $demoRNAFields['date'] = $row['date'];
                }

                if (!empty($row['followup']) && !is_null($row['followup'])) {
                    $demoRNAFields['followup'] = $row['followup'];
                }

                if (!empty($row['marked_on_e']) && !is_null($row['marked_on_e'])) {
                    $demoRNAFields['marked_on_e'] = $row['marked_on_e'];
                }

                if (!empty($row['marked_on_f']) && !is_null($row['marked_on_f'])) {
                    $demoRNAFields['marked_on_f'] = $row['marked_on_f'];
                }

                $data[] = $demoRNAFields;
                $countDemoAffectedRows++;
            }

            return [
                'data'  => $data,
                'count' => $countDemoAffectedRows,
                'last_inserted' => $lastInserted
            ];

        } else {
            
            $message = "No Demo Records to sync";
            return [
                'data'  => $message,
            ];
        }

        echo Logs::success("12MN ID Demo getDataFromFFA Process End: " . date('Y-m-d H:i:s') . "\n");
    }

    /**
     * Insert to staging table
     * @param $data
     * @param $lastInserted
     * @return array
     */
    public function insertIntoStaging($data, $lastInserted = null)
    {
        echo Logs::success("12MN ID Demo insertIntoStaging Process Start: " . date('Y-m-d H:i:s') . "\n");

        $count = 0;
        $objVN = new vn_charset_conversion();
        $country = $this->country['country_name'];
        foreach ($data as $demoRNAField) {
            if (!empty($demoRNAField['create_on']) && !is_null($demoRNAField['create_on']) || !empty($demoRNAField['update_on']) && !is_null($demoRNAField['update_on'])) {  
                // if (date('Y-m-d') == convert_to_datetime($demoRNAField['create_on'], 'Y-m-d') || date('Y-m-d') == convert_to_datetime($demoRNAField['update_on'], 'Y-m-d')) {
                    $results = $this->__checkRecordStaging($demoRNAField);

                    if (sqlsrv_num_rows($results) < 1) {
                      
                            $strColumns = implode(', ', array_keys($demoRNAField));
                            $strValues =  " '" . implode("', '", array_values($demoRNAField)) . "' ";
                            $demoInsertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES ({$strValues});";

                            $result = $this->exec_query($demoInsertQuery);
                            if ($result) {
                                $count += 1;
                            }
                        
                    } else {
                        $ffaId = $demoRNAField['ffa_id'];

                        $plan_on = (isset($demoRNAField['plan_on'])) ? 'plan_on = ' . "'{$demoRNAField['plan_on']}'," : null;
                        $approved_on = (isset($demoRNAField['approved_on'])) ? 'approved_on =' . "'{$demoRNAField['approved_on']}'," : null;
                        $closed_on = (isset($demoRNAField['closed_on'])) ? 'closed_on =' . "'{$demoRNAField['closed_on']}'," : null;
                        $date = isset($demoRNAField['date']) ? 'date =' . "'{$demoRNAField['date']}'," : null;
                        $temp_followup = $demoRNAField['temp_followup'];
                        $temp_execute = $demoRNAField['temp_execute'];
                        $ffaId = $demoRNAField['ffa_id'];
                        $markedByE = !is_null($demoRNAField['marked_by_e']) ? $demoRNAField['marked_by_e'] : 0;
                        $markedByF = !is_null($demoRNAField['marked_by_f']) ? $demoRNAField['marked_by_f'] : 0;
                        $markedOnE = isset($demoRNAField['marked_on_e']) ? 'marked_on_e =' . "'{$demoRNAField['marked_on_e']}'," : null;
                        $markedOnF = isset($demoRNAField['marked_on_f']) ? 'marked_on_f =' . "'{$demoRNAField['marked_on_f']}'," : null;
                        $team = (strtolower($country)=='vietnam') ? $objVN->convert( $demoRNAField['team']   ) : $demoRNAField['team'];
                        $team = (strtolower($country)=='thailand') ? $team : trim($team);

                        $demoUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->stagingTable]
                        SET 
                            update_on = '{$demoRNAField['update_on']}',
                            update_by = '{$demoRNAField['update_by']}',
                            {$plan_on}
                            plan_by   = '{$demoRNAField['plan_by']}',
                            {$approved_on}
                            approved_by = '{$demoRNAField['approved_by']}',
                            {$closed_on}
                            closed_by      = '{$demoRNAField['closed_by']}',
                            {$date}
                            team      = '{$team}',
                            deleted = {$demoRNAField['deleted']},
                            host_name      = '{$demoRNAField['host_name']}',
                            host_phone      = '{$demoRNAField['host_phone']}',
                            month      = '{$demoRNAField['month']}',
                            products      = '{$demoRNAField['products']}',
                            zone      = '{$demoRNAField['zone']}',
                            territory      = '{$demoRNAField['territory']}',
                            temp_followup   = '{$temp_followup}',
                            temp_execute   = '{$temp_execute}',
                            supervisore = '{$demoRNAField['supervisore']}',
                            supervisorf = '{$demoRNAField['supervisorf']}',
                            marked_by_e = '{$markedByE}',
                            marked_by_f = '{$markedByF}',
                            {$markedOnE}
                            {$markedOnF}
                            region      = '{$demoRNAField['region']}',
                            crop_id     = '{$demoRNAField['crop_id']}',
                            lat      = '{$demoRNAField['lat']}',
                            lng      = '{$demoRNAField['lng']}'
                        WHERE ffa_id = '$ffaId' AND report_table = '$this->reportTable';";

                        $result =  $this->exec_query($demoUpdateQuery);
                        if ($result) {
                            $count += sqlsrv_rows_affected($result);
                        }
                    }
                // }
            }
        }

        echo Logs::success("12MN ID Demo insertIntoStaging Process End: " . date('Y-m-d H:i:s') . "\n");

        return [
            'count' => $count
        ];
    }

    private function __checkLastRecordFFASync()
    {
        echo Logs::success("12MN ID Demo __checkLastRecordFFASync Process Start: " . date('Y-m-d H:i:s') . "\n");
        $sql = "SELECT id, last_insert_id, last_synced_date FROM $this->ffaSyncTable WHERE module = '$this->reportTable' AND action_name = 'create' ORDER BY id desc LIMIT 1";
        $results = $this->exec_query($sql);
        
        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }
        echo Logs::success("12MN ID Demo __checkLastRecordFFASync Process End: " . date('Y-m-d H:i:s') . "\n");
        return false;
    }

    /**
     * Checking record in staging table
     *
     * @param $data
     * @return object | array
     */
    private function __checkRecordStaging($data)
    {
        echo Logs::success("12MN ID Demo __checkRecordStaging Process Start: " . date('Y-m-d H:i:s') . "\n");

        $ffaId = $data['ffa_id'];
        $sql = "SELECT TOP 1 ffa_id, report_table FROM [$this->schemaName].[$this->stagingTable] WHERE report_table = '$this->reportTable' AND ffa_id = '$ffaId' ORDER BY id desc";
        $res = $this->exec_query($sql);

        echo Logs::success("12MN ID Demo __checkRecordStaging Process End: " . date('Y-m-d H:i:s') . "\n");
        return $res;
    }

    private function getDemoZoneRegion($territory)
    {
        echo Logs::success("12MN ID Demo getDemoZoneRegion Process Start: " . date('Y-m-d H:i:s') . "\n");

        $zoneSQL = "SELECT
            id,
            level
        FROM tbl_area_structure
        WHERE id = $territory LIMIT 1";

        $zone = $this->exec_query($zoneSQL);
        
        $zoneId = null;
        if ($zone->num_rows > 0) {
            $row = $zone->fetch_assoc();
            $zoneId = $row['level'];
        }
        
        $regionId = null;

        if ($zoneId != null) {
            $regionId = $this->getDemoRegion($zoneId);
        }

        echo Logs::success("12MN ID Demo getDemoZoneRegion Process End: " . date('Y-m-d H:i:s') . "\n");

        return [
            'zone'  => $zoneId,
            'region'    => $regionId
        ];
    }

    private function getDemoRegion($zoneId)
    {   
        echo Logs::success("12MN ID Demo getDemoRegion Process Start: " . date('Y-m-d H:i:s') . "\n");

        $regionSQL = "SELECT
            id,
            level
        FROM tbl_area_structure
        WHERE id = $zoneId LIMIT 1";

        $region = $this->exec_query($regionSQL);

        $regionId = null;
        if ($region->num_rows > 0) {
            $row = $region->fetch_assoc();
            $regionId = $row['level'];
        }

        echo Logs::success("12MN ID Demo getDemoRegion Process End: " . date('Y-m-d H:i:s') . "\n");

        return $regionId;
    }
    private function getSupervisor($ffa, $territory,$team)
    {   
        echo Logs::success("12MN ID Demo getSupervisor Process Start: " . date('Y-m-d H:i:s') . "\n");

        $supSQL =  "SELECT id, uterritory FROM users
        WHERE  active=1 AND team='$team' AND company='ZM' AND (uterritory<>'N;' ) order by id asc";
        
        $sup = $this->exec_query($supSQL);

        $supId = null;
        if ($sup->num_rows > 0) {
            $row = $sup->fetch_all();
            foreach($row as $s){
                    $territories = json_decode($s[1]);
                    if (in_array($territory, (array) $territories)) {
                         return $s[0];
                    }
            }
        }

        echo Logs::success("12MN ID Demo getSupervisor Process Start: " . date('Y-m-d H:i:s') . "\n");

        return $supId;
    }
}
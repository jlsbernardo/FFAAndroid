<?php

require_once dirname(__FILE__).'/RNA/Logs/Logs.php';
require_once dirname(__FILE__, 2).'/config/constant.php';
require_once dirname(__FILE__, 2).'/helpers/helpers.php';
require_once dirname(__FILE__, 2).'/helpers/vn_charset_conversion.php';

/**
 * Run staging scripts
 */
if (in_array('staging.php', $argv)) {
    if (count($argv) > 1) {
        if (in_array('--dev', $argv) || in_array('--staging', $argv)) {
            return $argv;
        } else {
            echo "\033[31m" . Logs::error("Invalid Arguments\n");
            exit();
        }
    }
}

/**
 * Run Demo Reports scripts
 */
if (in_array('demo_reports.php', $argv)) {

    if (in_array('--analytical', $argv)) {
        return $argv;
    } else {
        echo "\033[31m" . Logs::error("Incomplete command for demo reports \n");
        exit();
    }
}

/**
 * Run Farmer Meeting Reports scripts
 */
if (in_array('farmer_meeting_reports.php', $argv)) {

    if (in_array('--analytical', $argv)) {
        return $argv;
    } else {
        echo "\033[31m" . Logs::error("Incomplete command for farmer meeting reports \n");
        exit();
    }
}

/**
 * Run Demo Reports scripts
 */
if (in_array('retailer_reports.php', $argv)) {

    if (in_array('--analytical', $argv)) {
        return $argv;
    } else {
        echo "\033[31m" . Logs::error("Incomplete command for retailer visit reports \n");
        exit();
    }
}

/**
 * Run User Reports scripts
 */
if (in_array('user_reports.php', $argv)) {

    if (in_array('--analytical', $argv)) {
        return $argv;
    } else {
        echo "\033[31m" . Logs::error("Incomplete command for user reports \n");
        exit();
    }
}

/**
 * Run Portal Settings Reports scripts
 */
if (in_array('portal_settings_reports.php', $argv)) {

    if (in_array('--analytical', $argv)) {
        return $argv;
    } else {
        echo "\033[31m" . Logs::error("Incomplete command for portal settings reports \n");
        exit();
    }
}

/**
 * Run Portal Settings Reports scripts
 */
if (in_array('geo_hierarchy_region_reports.php', $argv)) {

    if (in_array('--analytical', $argv)) {
        return $argv;
    } else {
        echo "\033[31m" . Logs::error("Incomplete command for region reports \n");
        exit();
    }
}


/**
 * Run Portal Settings Reports scripts
 */
if (in_array('geo_hierarchy_zone_reports.php', $argv)) {

    if (in_array('--analytical', $argv)) {
        return $argv;
    } else {
        echo "\033[31m" . Logs::error("Incomplete command for zone reports \n");
        exit();
    }
}


/**
 * Run Portal Settings Reports scripts
 */
if (in_array('geo_hierarchy_territory_reports.php', $argv)) {

    if (in_array('--analytical', $argv)) {
        return $argv;
    } else {
        echo "\033[31m" . Logs::error("Incomplete command for territory reports \n");
        exit();
    }
}

/**
 * Run Portal Settings Reports scripts
 */
if (in_array('teams.php', $argv)) {

    if (in_array('--analytical', $argv)) {
        return $argv;
    } else {
        echo "\033[31m" . Logs::error("Incomplete command for teams reports \n");
        exit();
    }
}

/**
 * Run Complaints Reports scripts
 */
if (in_array('complaints.php', $argv)) {

    if (in_array('--analytical', $argv)) {
        return $argv;
    } else {
        echo "\033[31m" . Logs::error("Incomplete command for complaints reports \n");
        exit();
    }
}
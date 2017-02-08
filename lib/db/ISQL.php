<?php
 
/**
 * @file ISQL,php
 * @author checkking@foxmail.com
 * @date 2017-02-08
 * @brief  $Revision$
 *  
 */

interface Bd_Db_ISQL
{
    // return SQL text or false on error
    public function getSQL();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

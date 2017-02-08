<?php
/**
 * @file $FILE NAME$
 * @author $DoxygenToolkit_authorName$
 * @date 2017/02/08 14:19:45
 * @brief  $Revision$
 *  
 */

class Bd_Db_SQLAssember implements Bd_Db_ISQL
{
    const LIST_COM = 0;
    const LIST_AND = 1;
    const LIST_SET = 2;

    private $sql = NULL;
    private $db = NULL;

    public function __construct(Bd_DB $db)
    {
        $this->db = $db;
    }

    //select, select_count, insert, update, delete
    public function getSQL()
    {
        return $this->sql;
    }

    public function getSelect($tables, $fields, $conds = NULL, $options = NULL, $appends = NULL)
    {
        $sql = 'SELECT ';

        // 1. options
        if($options !== NULL)
        {
            $options = $this->__makeList($options, Bd_Db_SQLAssember::LIST_COM, ' ');
            if(!strlen($options))
            {
                $this->sql = NULL;
                return NULL;
            }
            $sql .= "$options ";
        }

        // 2. fields
        $fields = $this->__makeList($fields, Bd_Db_SQLAssember::LIST_COM);
        if(!strlen($fields))
        {
            $this->sql = NULL;
            return NULL;
        }
        $sql .= "$fields FROM ";

        // 3. from
        $tables = $this->__makeList($tables, Bd_Db_SQLAssember::LIST_COM);
        if(!strlen($tables))
        {
            $this->sql = NULL;
            return NULL;
        }
        $sql .= $tables;

        // 4. conditions
        if($conds !== NULL)
        {
            $conds = $this->__makeList($conds, Bd_Db_SQLAssember::LIST_AND);
            if(!strlen($conds))
            {
                $this->sql = NULL;
                return NULL;
            }
            $sql .= " WHERE $conds";
        }

        // 5. other append
        if($appends !== NULL)
        {
            $appends = $this->__makeList($appends, Bd_Db_SQLAssember::LIST_COM, ' ');
            if(!strlen($appends))
            {
                $this->sql = NULL;
                return NULL;
            }
            $sql .= " $appends";
        }

        $this->sql = $sql;
        return $sql;
    }

    public function getUpdate($table, $row, $conds = NULL, $options = NULL, $appends = NULL)
    {
        if(empty($row))
        {
            return NULL;
        }
        return $this->__makeUpdateOrDelete($table, $row, $conds, $options, $appends);
    }

    public function getDelete($table, $conds = NULL, $options = NULL, $appends = NULL)
    {
        return $this->__makeUpdateOrDelete($table, NULL, $conds, $options, $appends);
    }

    private function __makeUpdateOrDelete($table, $row, $conds, $options, $appends)
    {
        // 1. options
        if($options !== NULL)
        {
            if(is_array($options))
            {
                $options = implode(' ', $options);
            }
            $sql = $options;
        }

        // 2. fields
        // delete
        if(empty($row))
        {
            $sql = "DELETE $options FROM $table ";
        }
        // update
        else
        {
            $sql = "UPDATE $options $table SET ";
            $row = $this->__makeList($row, Bd_Db_SQLAssember::LIST_SET);
            if(!strlen($row))
            {
                $this->sql = NULL;
                return NULL;
            }
            $sql .= "$row ";
        }

        // 3. conditions
        if($conds !== NULL)
        {
            $conds = $this->__makeList($conds, Bd_Db_SQLAssember::LIST_AND);
            if(!strlen($conds))
            {
                $this->sql = NULL;
                return NULL;
            }
            $sql .= "WHERE $conds ";
        }

        // 4. other append
        if($appends !== NULL)
        {
            $appends = $this->__makeList($appends, Bd_Db_SQLAssember::LIST_COM, ' ');
            if(!strlen($appends))
            {
                $this->sql = NULL;
                return NULL;
            }
            $sql .= $appends;
        }

        $this->sql = $sql;
        return $sql;
    }

    public function getInsert($table, $row, $options = NULL, $onDup = NULL)
    {
        $sql = 'INSERT ';

        // 1. options
        if($options !== NULL)
        {
            if(is_array($options))
            {
                $options = implode(' ', $options);
            }
            $sql .= "$options ";
        }

        // 2. table
        $sql .= "$table SET ";

        // 3. clumns and values
        $row = $this->__makeList($row, Bd_Db_SQLAssember::LIST_SET);
        if(!strlen($row))
        {
            $this->sql = NULL;
            return NULL;
        }
        $sql .= $row;

        if(!empty($onDup))
        {
            $sql .= ' ON DUPLICATE KEY UPDATE ';
            $onDup = $this->__makeList($onDup, Bd_Db_SQLAssember::LIST_SET);
            if(!strlen($onDup))
            {
                $this->sql = NULL;
                return NULL;
            }
            $sql .= $onDup;
        }
        $this->sql = $sql;
        return $sql;
    }

    private function __makeList($arrList, $type = Bd_Db_SQLAssember::LIST_SET, $cut = ', ')
    {
        if(is_string($arrList))
        {
            return $arrList;
        }

        $sql = '';

        // for set in insert and update
        if($type == Bd_Db_SQLAssember::LIST_SET)
        {
            foreach($arrList as $name => $value)
            {
                if(is_int($name))
                {
                    $sql .= "$value, ";
                }
                else
                {
                    if(!is_int($value))
                    {
                        if($value === NULL)
                        {
                            $value = 'NULL';
                        }
                        else
                        {
                            $value = '\''.$this->db->escapeString($value).'\'';
                        }
                    }
                    $sql .= "$name=$value, ";
                }
            }
            $sql = substr($sql, 0, strlen($sql) - 2);
        }
        // for where conds
        else if($type == Bd_Db_SQLAssember::LIST_AND)
        {
            foreach($arrList as $name => $value)
            {
                if(is_int($name))
                {
                    $sql .= "($value) AND ";
                }
                else
                {
                    if(!is_int($value))
                    {
                        if($value === NULL)
                        {
                            $value = 'NULL';
                        }
                        else
                        {
                            $value = '\''.$this->db->escapeString($value).'\'';
                        }
                    }
                    $sql .= "($name $value) AND ";
                }
            }
            $sql = substr($sql, 0, strlen($sql) - 5);
        }
        else
        {
            $sql = implode($cut, $arrList);
        }

        return $sql;
    }
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

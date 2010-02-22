function getCredentialSetting {
  name=$1
  eval "val=\$$name"
  if [ -z "$val" ] ; then
    if [ -f "$bin"/credentials.sh ] ; then
      val=`cat "$bin"/credentials.sh | grep $name | awk 'BEGIN { FS="=" } { print $2; }'`
      if [ -z "$val" ] ; then
        echo -n "$name: "
        read -e val
        echo "$name=$val" >> "$bin"/credentials.sh
      fi
    else
      echo -n "$name: "
      read -e val
      echo "$name=$val" >> "$bin"/credentials.sh
    fi
    eval "$name=$val"
  fi
}

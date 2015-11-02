#!/bin/bash

### Test for root
if [ "$UID" -ne "0" ]
then
  echo "Must be root to run this script."
  exit 1
fi  
echo ""
echo "=== RadarGun ==="
echo " This script is to be used on environments where slave nodes are provisioned via PXE boot"
echo " and a master provides this PXE image and also acts as a DHCP server.  This script should"
echo " *only* be run on the master, and it will attempt to detect the slave nodes' IP addresses"
echo " and give them friendly names by adding them to /etc/hosts"
echo ""

SLAVE_PREFIX=slave
SHOW_WARNING=true


help_and_exit() {
  echo "Usage: "
  echo '  $ detect_slave_ips.sh [-p SLAVE_PREFIX] [-y]'
  echo ""
  echo "   -p   Provides a prefix to all slave names entered in /etc/hosts"
  echo "        Defaults to '$SLAVE_PREFIX'"
  echo ""
  echo "   -y   Answers 'yes' to all questions asked"
  echo ""
  echo "   -h   Displays this help screen"
  echo ""
  exit 0
}

### read in any command-line params
while ! [ -z $1 ] 
do
  case "$1" in
    "-p")
      SLAVE_PREFIX=$2
      ## Do an extra shift here since we have popped off the next param as well
      shift 
      ;;
    "-y")
      SHOW_WARNING=false
      ;;
    *)
      help_and_exit
      ;;
  esac
  shift
done

echo "Slave prefix: $SLAVE_PREFIX"

if [ $SHOW_WARNING="true" ] ; then
  echo "Are you sure you wish to continue?  Any key to continue, CTRL-C to quit."
  read any_key
fi

dhcp_leases_file=/var/lib/dhcpd/dhcpd.leases

if ! [ -e $dhcp_leases_file ] ; then
  echo "FATAL: Cannot locate $dhcp_leases_file to detect assigned IPs!"
  echo "       Quitting!"
  exit 1
fi

grep "^lease " $dhcp_leases_file | sed -e "s/^lease //g" -e "s/ {.*$//g" > /tmp/ip_address_guesses

echo "" >> /etc/hosts
echo "#### GENERATED BY RadarGun" >> /etc/hosts
echo "" >> /etc/hosts

./unique_ips.py /tmp/ip_address_guesses $SLAVE_PREFIX > /tmp/slave_addresses
cat /tmp/slave_addresses >> /etc/hosts

echo "192.168.0.254 master server1" >> /tmp/slave_addresses
## Install slave hosts file for each slave
for slavename in `sed -e "s/^.* //g" /tmp/slave_addresses` ; do
  scp -q -o "StrictHostKeyChecking false" /tmp/slave_addresses root@$slavename:/tmp/
  ssh -q -o "StrictHostKeyChecking false" root@$slavename "cat /tmp/slave_addresses >> /etc/hosts ; hostname $slavename ; rm /tmp/slave_addresses" &
done

rm /tmp/ip_address_guesses
rm /tmp/slave_addresses

echo " ... Done!  Please check /etc/hosts to verify that slaves have been properly entered!"
echo ""
echo ""
exit 0

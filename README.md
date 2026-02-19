sudo lsof /dev/bus/usb/$(lsusb | grep RTL | awk '{print $2"/"$4}' | tr -d :)

sudo kill -9 4796

setsid node app27.js > /dev/null 2>&1 &


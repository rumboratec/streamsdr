sudo lsof /dev/bus/usb/$(lsusb | grep RTL | awk '{print $2"/"$4}' | tr -d :)
sudo kill -9 4796

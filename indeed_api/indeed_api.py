
import logging
import csv
import urllib2
import xml.etree.cElementTree as ET

log = logging.getLogger("indeed_api")
FILENAME='zip.csv'
#FILENAME='ziptest.csv'
OUTFILE = 'output/Indeed_Zip{0}_{1}_{2}.xml'
BASEURL = 'http://api.indeed.com/ads/apisearch?publisher=1986175114212579&q=hadoop&l={0}&sort=&radius=&st=&jt=&start={1}&limit=&fromage=&filter=&latlong=1&co=us&chnl=&userip=1.2.3.4&useragent=Mozilla/%2F4.0%28Firefox%29&v=2'

def read_zip():
    zipcodes = []
    zipfile = csv.reader(open(FILENAME, 'rb'), delimiter=',')
    for row in zipfile:
        zipcodes.append(row)
    return zipcodes

def write_xml(xml_data, zip, start):
    outfile = OUTFILE.format(zip, start, start+25)
    with open(outfile, 'w') as f:
         f.write(xml_data)


def _run():
    log.info("Start")
    # get zip
    zipcodes = read_zip()
    print zipcodes
    if len(zipcodes) > 0:
        log.info("Zip file is loaded. Zipcode count:{0}".format(len(zipcodes)))

    # call Api
    try:
        for zip_row in zipcodes:
            zip = zip_row[0]
            log.info("Processing...{0}".format(zip))

            try:
                url = BASEURL.format(zip, 0) # create url
                xml_data = urllib2.urlopen(url, timeout=60).read() # call api

                root = ET.fromstring(xml_data) # parsing xml file to get totalresults value
                totalresults =int(root.find('totalresults').text)

                if totalresults == 0:
                    log.info("No record in this zip area. {0}".format(zip))
                    continue
                elif totalresults > 1025:
                    starts = range(0, 1025, 25)   # [0, 25, 50, 75, 100, 125, 150, 175, 200, 225,...1000]
                else:
                    starts = range(0, totalresults, 25) # max = totalresults
                log.info("{0} job found in {1}, {2}".format(totalresults, zip_row[1] , zip_row[2]))

            except urllib2.HTTPError, err:
                log.warning("Api error, bad request.")
                return
            except Exception as e:
                log.warning("Api error. {0}".format(e.message) )
                return

            # main loop
            for st in starts:
                url = BASEURL.format(zip, st)
                xml_data = urllib2.urlopen(url, timeout=60).read()
                if len(xml_data) == 0:
                    log.worning("Xml is empty. Api error? zip:{0}, start:{1}".format(zip, st))
                    break

                # write to xml file
                try:
                    write_xml(xml_data, zip, st)
                except Exception as e:
                    log.warning("Writing file is failed. {0}".format(e.message) )
                    return

    except urllib2.HTTPError, err:
        log.warning("Api error, bad request.")
        return
    except Exception as e:
        log.warning("Api error. {0}".format(e.message) )
        return



if __name__ == '__main__':
    # logging setup
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(name)s: %(levelname)s: %(message)s')
    # output to logfile
    filehandler = logging.FileHandler("indeed_api.log")  # permanent record of details related to IP's that resulted in an RBL entry.
    filehandler.setFormatter(formatter)
    log.addHandler(filehandler)
    #output to console
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    log.addHandler(consoleHandler)
    _run()
    def parse(self, data, xml):
        for k, v in data.iteritems():
            k = k.replace("_", "-")
            if type(v) is list:
                xml = "%s>" % (xml,)
                for v1 in v:
                    xml = "%s<%s" % (xml, k)
                    xml = self.parse(v1, xml)
                    if (k == "file"):
                        xml = "%s/>" % (xml,)
                    else:
                        xml = "%s</%s>" % (xml, k)
            else:
                if k == "lfn":
                    k = "name"
                elif k == "size":
                    k = "bytes"
                if (k == "name" or k == "is-open" or k == "is-transient" or k == "bytes" or k== "checksum"):
                    xml = '%s %s="%s"' % (xml, k, v)
        return xml

    def xmlData(self, datasets=[], instance='prod'):
        name = "xmlData"
        if not datasets:
            self.logger.error(name, "Need to pass at least one dataset")
            return 1, "Error"
        xml = '<data version="2">'
        xml = '%s<%s name="https://cmsweb.cern.ch/dbs/%s/global/DBSReader">' % (xml, 'dbs', instance)
        for dataset in datasets:
            check = 1
            check, response = self.data(dataset=dataset, level='file', instance=instance)
            if check:
                return 1, "Error"
            data = response.get('phedex').get('dbs')
            if not data:
                return 1, "Error"
            xml = "%s<%s" % (xml, 'dataset')
            data = data[0].get('dataset')
            xml = self.parse(data[0], xml)
            xml = "%s</%s>" % (xml, 'dataset')
        xml = "%s</%s>" % (xml, 'dbs')
        xml_data = "%s</data>" % (xml,)
        return 0, xml_data
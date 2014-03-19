#!/usr/bin/python
import requests
import pprint
import json

class ElasticSinkSetup(object):

	def __init__(self, host):
		self.host = host

	def create_index(self):
		cond = {
			"type":"upload",
			"analysis":{
				"analyzer": {
					"ngram2_36_analyzer":{
						"type" : "custom",
						"tokenizer" : "standard",
						"filter" : ["lowercase", "nGram2_36"]
					}
				},
				"filter" : {
					"nGram2_36" : {
						"type" : "nGram",
						"min_gram" : 2,
						"max_gram" : 36
					}
				}
			}
		}
		r = requests.post("http://%s/nzb" % self.host, data=json.dumps(cond))
		return (r.status_code, r.json())

	def create_upload_type(self):
		conf = {
			"upload": {
				"_all": {
					"index_analyzer":"ngram2_36_analyzer",
				},
				"dynamic_templates":[
					{
            "types_template":{
              "mapping":{
                "index":"no",
                "store":"yes"
              },
              "path_match":"types.*"
            }
          }
        ],
				"properties": {
					"poster": {
						"type": "string",
						"store":True,
					},
					"subject": {
						"type": "string",
						"index_analyzer":"ngram2_36_analyzer",
						"store":True,
					},
					"date": {
						"type": "date",
						"format": "date_time_no_millis",
						"include_in_all": False,
						"store":True,
					},
					"group": {
						"type":"string",
						"include_in_all": False,
						"store":True,
					},
					"dmca": {
						"type":"boolean",
						"include_in_all": False,
						"null_value": False,
					},
					"length": {
						"type":"integer",
						"include_in_all": False,
						"store":True,
					},
					"complete": {
						"type":"integer",
						"include_in_all": False,
						"store":True,
					},
					"completion": {
						"type":"double",
						"include_in_all": False,
						"store":True,
					},
					"size": {
						"type":"long",
						"include_in_all": False,
						"store":True,
					},
					"fileprefix": {
						"type":"string",
						"include_in_all": False,
						"index_name":"filename",
						"store":True,
					}
				}
			}
		}
		r = requests.post("http://%s/nzb/upload/_mapping" % self.host, data=json.dumps(conf))
		return (r.status_code, r.json())

	def create_file_type(self):
		conf = {
			"file": {
				"dynamic": False,
				"_all": {
					"index_analyzer":"standard",
				},
				"_parent":{
      		"type" : "upload"
    		},
				"properties": {
					"poster": {
						"type": "string",
					},
					"subject": {
						"type": "string",
						"index_analyzer":"standard",
					},
					"date": {
						"type": "date",
						"format": "date_time_no_millis",
						"include_in_all": False,
					},
					"group": {
						"type":"string",
						"include_in_all": False,
					},
					"length": {
						"type":"integer",
						"include_in_all": False,
					},
					"complete": {
						"type":"integer",
						"include_in_all": False,
					},
					"completion": {
						"type":"double",
						"include_in_all": False,
					},
					"size": {
						"type":"long",
						"include_in_all": False,
					},
				 }
			 }
		}
		r = requests.post("http://%s/nzb/file/_mapping" % self.host, data=json.dumps(conf))
		return (r.status_code, r.json())

	def create_segment_type(self):
		conf = {
			"segment": {
				"dynamic": False,
				"_all": {
					"index_analyzer":"standard",
				},
				"_id" : {
          "path": "message_id",
        },
				"_parent":{
      		"type" : "file"
    		},
				"properties": {
					"poster": {
						"type": "string",
					},
					"subject": {
						"type": "string",
						"index_analyzer":"standard",
					},
					"date": {
						"type": "date",
						"format": "date_time_no_millis",
						"include_in_all": False,
					},
					"group": {
						"type":"string",
						"include_in_all": False,
					},
					"size": {
						"type":"long",
						"include_in_all": False,
					},
					"server_article_id": {
						"type":"long",
						"include_in_all": False,
					}
					"added": {
						"type": "date",
						"format": "date_time_no_millis",
						"include_in_all": False,
					}
				 }
			 }
		}
		r = requests.post("http://%s/nzb/segment/_mapping" % self.host, data=json.dumps(conf))
		return (r.status_code, r.json())

	def run(self):
		resp = self.create_index()
		if resp[0] != 200:
			if not (resp[0] == 400 and resp[1]['error'].find("IndexAlreadyExistsException") == 0):
				print "An error occured."
				pprint.pprint(resp[1])
				return
		pprint.pprint(resp[1])

		resp = self.create_upload_type()
		if resp[0] != 200:
			print "An error occured."
			pprint.pprint(resp[1])
			return
		pprint.pprint(resp[1])

		resp = self.create_file_type()
		if resp[0] != 200:
			print "An error occured."
			pprint.pprint(resp[1])
			return
		pprint.pprint(resp[1])


		resp = self.create_segment_type()
		if resp[0] != 200:
			print "An error occured."
			pprint.pprint(resp[1])
			return
		pprint.pprint(resp[1])



if __name__ == "__main__":
	ess = ElasticSinkSetup("localhost:9200")
	ess.run()




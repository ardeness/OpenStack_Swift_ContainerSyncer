import collections
import iso8601
import time
import calendar
import re
import json
import pycurl
import tempfile
import signal
from StringIO import StringIO

class ContainerManager(object) :

	def __init__(self, keystoneurl, swifturl, account, tenant, username, password, \
		     authmethod="keystone", isprotected=True) :
		self.keystoneurl	= keystoneurl
		self.swifturl		= swifturl
		self.account		= account
		self.authtoken		= None
		self.authinfo		= None
		self.containerlist	= None
		self.objectdict		= {}
		self.authmethod		= authmethod
		self.isprotected	= isprotected

		self.tenant		= tenant
		self.username		= username
		self.password		= password

		self.Log = self.defaultLog
		self.setAlarm = self.defaultAlarm

		self.storagetoken	= None

		self.success		= 0
		self.fail		= 0
		self.progress		= 0

		self.updateProgress	= self.defaultUpdateProgress

		if not self.swifturl[-1] == '/' :
			self.swifturl = self.swifturl+'/'

		if not self.keystoneurl[-1] == '/' :
			self.keystoneurl = self.keystoneurl+'/'



	def revokeHandler(self, signnum, frame) :
		self.getToken()



	def defaultAlarm(self, alarmtime) :
		signal.signal(signal.SIGALRAM, self.revokeHandler)
		currenttime = int(time.time())
		signal.alarm(alarmtime - currenttime)



	def getToken(self) :
		if self.authmethod == "keystone" :
			tenant		= {'tenantName':self.tenant}
			username	= {'username':self.username}
			password	= {'password':self.password}

			username.update(password)
			credentials={"passwordCredentials":username}
			tenant.update(credentials)
			auth={"auth":tenant}
			authinfo = json.dumps(auth)

			token_response = StringIO()

			token_curl = pycurl.Curl()
			token_curl.setopt(pycurl.URL, self.keystoneurl)
			token_curl.setopt(pycurl.HTTPHEADER, ['Content-Type: application/json'])
			token_curl.setopt(pycurl.POST, 1)
			token_curl.setopt(pycurl.POSTFIELDS, authinfo)
			token_curl.setopt(pycurl.WRITEFUNCTION, token_response.write)
			token_curl.perform()
			http_status = token_curl.getinfo(pycurl.HTTP_CODE)
			token_curl.close()

			if http_status == 200 or http_status == 203 :
				token_jsondata = json.load(StringIO(token_response.getvalue()))
				self.authtoken = str(token_jsondata['access']['token']['id'])
				expiretime = token_jsondata['access']['token']['expires']
				timestamp = iso8601.parse_date(expiretime).utctimetuple()
				timestamp = int(calendar.timegm(timestamp))

				# Raise alarm 10 minutes before token expires
				alarmtime = timestamp - 600
				currenttime = int(time.time())
				self.setAlarm(alarmtime)
				self.Log("Requesting keystone auth token completed")
				return True
			else :
				errmsg = None

				if http_status == 500 :
					errmsg = "Identity fault"
				elif http_status == 400 :
					errmsg = "Bad request"
				elif http_status == 403 :
					errmsg = "User disabled"
				elif http_status == 403 :
					errmsg = "Fobbiden"
				elif http_status == 405 : 
					errmsg = "Bad method"
				elif http_status == 413 :
					errmsg = "Over limit"
				elif http_status == 503 :
					errmsg = "Service unavailable"
				elif http_status == 404 :
					errmsg = "Item not found"
				else :
					errmsg = "Unknown error"

				errmsg = "ERROR : Keystone auth token request failed : " + str(http_status) + " " + errmsg

				self.Log(errmsg)
				self.authtoken = None

				return False

		elif self.authmethod == 'tempauth' :
			token_response = StringIO()
			token_curl = pycurl.Curl()
			token_curl.setopt(pycurl.URL, self.keystoneurl)
			token_curl.setopt(pycurl.HTTPHEADER,['X-Auth-User:'+self.username,'X-Auth-Key:'+self.password])
			token_curl.setopt(pycurl.CUSTOMREQUEST, "GET")
			token_curl.setopt(pycurl.HEADERFUNCTION, token_response.write)
			token_curl.perform()
			http_status = token_curl.getinfo(pycurl.HTTP_CODE)
			token_curl.close()

			if http_status == 200 or http_status == 203 :
				value = token_response.getvalue()
				auth_dict = dict(re.findall(r"(?P<name>.*?): (?P<value>.*?)\r\n", value))
				self.authtoken = str(auth_dict['X-Auth-Token'])
				self.storagetoken = str(auth_dict['X-Storage-Token'])
				self.setAlarm(9999999999)
				self.Log("Requesting tempauth token completed")
				return True

			else :
				errmsg = None

				if http_status == 500 :
					errmsg = "Identity fault"
				elif http_status == 400 :
					errmsg = "Bad request"
				elif http_status == 403 :
					errmsg = "User disabled"
				elif http_status == 403 :
					errmsg = "Fobbiden"
				elif http_status == 405 : 
					errmsg = "Bad method"
				elif http_status == 413 :
					errmsg = "Over limit"
				elif http_status == 503 :
					errmsg = "Service unavailable"
				elif http_status == 404 :
					errmsg = "Item not found"
				else :
					errmsg = "Unknown error"

				errmsg = "ERROR : Keystone auth token request failed : " + str(http_status) + " " + errmsg

				self.Log(errmsg)
				self.authtoken = None

				return False

		else :
			self.Log("ERROR : Unknown auth method : %s" % self.authmethod)
			return False



	def getContainerList(self, nameonly = True) :
		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		headlist = ['X-Auth-Token:'+self.authtoken]

		container_response = StringIO()
		container_curl = pycurl.Curl()
		container_curl.setopt(pycurl.URL, self.swifturl + self.account)
		container_curl.setopt(pycurl.HTTPHEADER, headlist)
		container_curl.setopt(pycurl.WRITEFUNCTION, container_response.write)
		container_curl.perform()
		http_status = container_curl.getinfo(pycurl.HTTP_CODE)
		container_curl.close()

		if not http_status == 200 and not http_status == 204 :
			return False

		container_rawdata = container_response.getvalue()

		containerlist = [container for container in container_rawdata.split('\n') if container.strip() != '']

		if nameonly :
			return containerlist

		containerdictlist = {}

		for containername in containerlist :
			metainfo = self.getContainerMetadata(containername)
			if metainfo :
				timestamp = metainfo['Date']
				timestamp= time.strptime(timestamp,"%a, %d %b %Y %H:%M:%S %Z")
				timestamp = int(calendar.timegm(timestamp))
				containerdictlist[containername] = timestamp
		return containerdictlist



	def getObjectCount(self, containername) :
		status = True

		containermeta = self.getContainerMetadata(containername)

		if containermeta :
			return int(containermeta['X-Container-Object-Count'])

		else :
			return False



	def getObjectList(self, containername, nameonly=False) :
		total_objlist_length = 0
		total_objname_list = []
		current_objlist_length = 0
		lastobjname = None

		status = True
		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		headlist = ['X-Auth-Token:'+self.authtoken]

		meta_response = StringIO()
		meta_curl = pycurl.Curl()
		meta_curl.setopt(pycurl.URL, self.swifturl + self.account + '/' + containername)
		meta_curl.setopt(pycurl.HTTPHEADER, headlist)
		meta_curl.setopt(pycurl.HEADERFUNCTION, meta_response.write)
		meta_curl.setopt(pycurl.NOBODY, 1)
		meta_curl.perform()
		http_status = meta_curl.getinfo(pycurl.HTTP_CODE)
		meta_curl.close()

		if http_status == 200 or http_status == 204 :
			value = meta_response.getvalue()
			meta_dict = dict(re.findall(r"(?P<name>.*?): (?P<value>.*?)\r\n", value))
			total_objlist_length = int(meta_dict['X-Container-Object-Count'])

		else :
			self.Log("ERROR : Cannot retrieve the information of container %s" % containername)
			return False


		headlist = ['X-Auth-Token:'+self.authtoken]
		marker = ""

		while True :
			objlist_response = StringIO()
			objlist_curl = pycurl.Curl()
			objlist_url = self.swifturl + self.account + '/' + containername + marker
			objlist_curl.setopt(pycurl.URL, objlist_url)
			objlist_curl.setopt(pycurl.HTTPHEADER, headlist)
			objlist_curl.setopt(pycurl.WRITEFUNCTION, objlist_response.write)
			objlist_curl.perform()
			http_status = objlist_curl.getinfo(pycurl.HTTP_CODE)
			objlist_curl.close()

			if http_status == 200 or http_status == 204:
				objname_list = objlist_response.getvalue().split('\n')
				objname_list = [objectname for objectname in objname_list if objectname.strip() != '']
				if not len(objname_list) :
					break

				lastobjname = objname_list[-1]
				marker = '?marker='+lastobjname
				total_objname_list.extend(objname_list)

			elif http_status == 404 :
				self.Log("ERROR : Container %s not found" % containername)
				return False

			else :
				self.Log("ERROR : Unknown error occured while getting object list of container %s" \
					% containername)
				return False

			objlist_response.close()

			current_objlist_length = len(total_objname_list)
			if current_objlist_length >= total_objlist_length :
				break

		if nameonly == True :
			return total_objname_list

		objdict = {}
		for objectname in total_objname_list :
			objmeta_dict = self.getObjectMetadata(containername, objectname)
			if objmeta_dict == False :
				self.Log("Cannot retrive meta information of object %s" % objectname)
			else :
				objtype = None
				if 'X-Object-Manifest' in objmeta_dict :
					objtype = 'DLO'
				elif 'X-Static-Large-Object' in objmeta_dict :
					objtype = 'SLO'
				else :
					objtype = 'NORMAL'

				timestamp= time.strptime(objmeta_dict['Last-Modified'],"%a, %d %b %Y %H:%M:%S %Z")
				timestamp= int(calendar.timegm(timestamp))
				objdict[objectname]={'lastmodifiedtime':timestamp, 'type':objtype}

		return objdict


	def getContainerMetadata(self, containername) :
		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		self.Log("Get metadata from container %s" % containername)

		headlist = ['X-Auth-Token:'+self.authtoken]

		containermeta_response = StringIO()
		containermeta_curl = pycurl.Curl()
		containermeta_curl.setopt(	pycurl.URL, \
					self.swifturl + self.account + '/' + containername)
		containermeta_curl.setopt(pycurl.HTTPHEADER, headlist)
		containermeta_curl.setopt(pycurl.CUSTOMREQUEST, "GET")
		containermeta_curl.setopt(pycurl.HEADERFUNCTION, containermeta_response.write)
		containermeta_curl.setopt(pycurl.NOBODY, 1)
		containermeta_curl.perform()
		http_status = containermeta_curl.getinfo(pycurl.HTTP_CODE)
		containermeta_curl.close()

		if http_status == 200 or http_status == 204 :
			value = containermeta_response.getvalue()
			containermeta_dict = dict(re.findall(r"(?P<name>.*?): (?P<value>.*?)\r\n", value))
			return containermeta_dict

		elif http_status == 404 :
			self.Log("Container %s not found" % containername)
			return False

		else :
			self.Log("Unknown error occured while getting information of %s" % containername)
			return False



	def createContainer(self, containername) :
		if self.isprotected :
			return
	
		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		headlist = ['X-Auth-Token:'+self.authtoken]

		container_response = StringIO()
		container_curl = pycurl.Curl()
		container_curl.setopt(pycurl.URL, self.swifturl + self.account + '/' + containername)
		container_curl.setopt(pycurl.HTTPHEADER, headlist)
		container_curl.setopt(pycurl.CUSTOMREQUEST, "PUT")
		container_curl.setopt(pycurl.WRITEFUNCTION, container_response.write)
		container_curl.perform()
		http_status = container_curl.getinfo(pycurl.HTTP_CODE)
		container_curl.close()

		if http_status == 201 or http_status == 204 :
			self.Log("Creating container %s completed" % containername)
			return True
		else :
			self.Log("ERROR : Unknown error occrued while creating container %s" % containername)
			return False



	def deleteContainer(self, containername) :
		if self.isprotected :
			return

		status = True 

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		logmsg = ""
		# Delete objects in the container
		objnamelist = self.getObjectList(containername, nameonly=True)
		if objnamelist :
			objnamelist = sorted(objnamelist, reverse=True)
			length = len(objnamelist)
			completed = 0
			successed = 0
			failed    = 0

			self.Log("Deleting %s objects in container %s" % (str(length), containername))
			for objname in objnamelist :
				status = self.deleteLObject(containername, objname)

				if status :
					successed = successed + 1
					self.updateProgress(1, 0)
					logmsg = "deleted"
				else      :
					failed = failed + 1
					self.updateProgress(0, 1)
					logmsg = "not deleted"
				completed = completed + 1
				self.Log("Object %s %s - Success : %s, Failed : %s, Total : %s" \
					  % ( objname, logmsg, str(successed), str(failed), str(completed)))

		else :
			self.Log("Container %s has no objects to delete" % containername)


		# Delete container itself
		headlist = ['X-Auth-Token:'+self.authtoken]

		container_response = StringIO()
		container_curl = pycurl.Curl()
		container_curl.setopt(pycurl.URL, self.swifturl + self.account + '/' + containername)
		container_curl.setopt(pycurl.HTTPHEADER, headlist)
		container_curl.setopt(pycurl.CUSTOMREQUEST, "DELETE")
		container_curl.setopt(pycurl.WRITEFUNCTION, container_response.write)
		container_curl.perform()
		http_status = container_curl.getinfo(pycurl.HTTP_CODE)
		container_curl.close()

		if http_status == 204 :
			self.Log("Container %s deleted" % containername)
			return True
		elif http_status == 404 :
			self.Log("Container %s not found" % containername)
			return False
		elif http_status == 409 :
			self.Log("Deleting container %s conflicted" % containername)
			return False


	def getObjectMetadata(self, containername, objectname) :
		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		self.Log("Get metadata from object %s in container %s" % (objectname, containername))

		headlist = ['X-Auth-Token:'+self.authtoken]

		objmeta_response = StringIO()
		objmeta_curl = pycurl.Curl()
		objmeta_curl.setopt(	pycurl.URL, \
					self.swifturl + self.account + '/' + containername + '/' + objectname)
		objmeta_curl.setopt(pycurl.HTTPHEADER, headlist)
		objmeta_curl.setopt(pycurl.HEADERFUNCTION, objmeta_response.write)
		objmeta_curl.setopt(pycurl.NOBODY, 1)
		objmeta_curl.perform()
		http_status = objmeta_curl.getinfo(pycurl.HTTP_CODE)
		objmeta_curl.close()

		if http_status == 200 :
			value = objmeta_response.getvalue()
			objmeta_dict = dict(re.findall(r"(?P<name>.*?): (?P<value>.*?)\r\n", value))
			return objmeta_dict

		elif http_status == 404 :
			self.Log("Object %s not found in container %s" % (objectname, containername))
			return False

		else :
			self.Log("Unknown error occured while getting information of %s" % objectname)
			return False



	def getObject(self, containername, objectname) :
		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		self.Log("Get blob from object %s in container %s" % (objectname, containername))

		headlist = ['X-Auth-Token:'+self.authtoken]

		tempfile_name = objectname.replace('/','_')
		object_response = tempfile.NamedTemporaryFile()
		object_curl = pycurl.Curl()
		object_curl.setopt(pycurl.URL, self.swifturl + self.account + '/' + containername + '/' + objectname)
		object_curl.setopt(pycurl.HTTPHEADER, headlist)
		object_curl.setopt(pycurl.WRITEFUNCTION, object_response.write)
		object_curl.perform()
		http_status = object_curl.getinfo(pycurl.HTTP_CODE)
		object_curl.close()


		if http_status == 404 :
			self.Log("Object %s not found in container %s" % (objectname, containername))
			return False

		elif http_status == 200 :
			return object_response

		else :
			self.Log("Unknown error occured while getting contents of object %s" % objectname)
			return False



	def putObject(self, containername, objectname, blob) :
		if self.isprotected :
			return

		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		self.Log("Uploading object blob %s to container %s" % (objectname, containername))

		size = blob.tell()
		blob.seek(0, 0)

		headlist = ['X-Auth-Token:'+self.authtoken, 'Content-Length:'+str(size)]

		object_curl = pycurl.Curl()
		object_curl.setopt(pycurl.URL, self.swifturl + self.account + '/' + containername + '/' + objectname)
		object_curl.setopt(pycurl.HTTPHEADER, headlist)
		object_curl.setopt(pycurl.CUSTOMREQUEST, "PUT")
		object_curl.setopt(pycurl.UPLOAD, 1)
		object_curl.setopt(pycurl.READFUNCTION, blob.read)
		object_curl.perform()
		http_status = object_curl.getinfo(pycurl.HTTP_CODE)
		object_curl.close()

		if http_status == 201 :
			#self.updateProgress(1,0)
			self.Log("Uploading object %s in container %s completed" % (objectname, containername))
			return True
		elif http_status == 408 :
			#self.updateProgress(0,1)
			self.Log("ERROR : Request timeout for writing object %s in container %s" % (objectname, containername))
			return False
		elif http_status == 411 :
			#self.updateProgress(0,1)
			self.Log("ERROR : Contents length required for object %s in container %s" \
				% (objectname, containername))
			return False
		elif http_status == 422 :
			#self.updateProgress(0,1)
			self.Log("ERROR : Unprocessable object %s in container %s" % (objectname, containername))
			return False
		else :
			#self.updateProgress(0,1)
			self.Log("ERROR : Unknown error occured while writing object %s in container %s" \
				% (objectname, containername))
			return False



	def deleteObject(self, containername, objectname) :
		if self.isprotected :
			return

		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		headlist = ['X-Auth-Token:'+self.authtoken]

		object_response = StringIO()
		object_curl = pycurl.Curl()
		url= self.swifturl + self.account + '/' + containername + '/' + objectname
		object_curl.setopt(pycurl.URL, self.swifturl + self.account + '/' + containername + '/' + objectname)
		object_curl.setopt(pycurl.HTTPHEADER, headlist)
		object_curl.setopt(pycurl.CUSTOMREQUEST, "DELETE")
		object_curl.setopt(pycurl.WRITEFUNCTION, object_response.write)
		object_curl.perform()
		http_status = object_curl.getinfo(pycurl.HTTP_CODE)
		object_curl.close()

		if http_status == 200 or http_status == 204 :
			self.Log("Object %s in container %s deleted" % (objectname, containername))
			return True
		else :
			self.Log("ERROR : Deleting object %s in container %s failed" % (objectname, containername))
			return False



	def deleteLObject(self, containername, objectname) :
		if self.isprotected :
			return

		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		headlist = ['X-Auth-Token:'+self.authtoken]

		object_response = StringIO()
		object_curl = pycurl.Curl()
		object_url = self.swifturl+self.account+'/'+containername+'/'+objectname+'?multipart-manifest=delete'
		object_curl.setopt(pycurl.URL, object_url)
		object_curl.setopt(pycurl.HTTPHEADER, headlist)
		object_curl.setopt(pycurl.CUSTOMREQUEST, "DELETE")
		object_curl.setopt(pycurl.WRITEFUNCTION, object_response.write)
		object_curl.perform()
		http_status = object_curl.getinfo(pycurl.HTTP_CODE)
		object_curl.close()

		if http_status == 200 or http_status == 204 :
			self.Log("Object %s in container %s deleted" % (objectname, containername))
			return True
		else :
			self.Log("ERROR : Deleting object %s in container %s failed" % (objectname, containername))
			return False



	def putSLOManifest(self, containername, objectname, manifest) :
		if self.isprotected :
			return

		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		data = json.dumps(manifest)

		headlist = ['X-Auth-Token:'+self.authtoken, 'Content-Type: application/json',\
			'X-Static-Large-Object: True']

		obj_response = StringIO()
		obj_curl = pycurl.Curl()
		obj_curl.setopt(pycurl.URL,self.swifturl+self.account+'/'+containername+'/'+objectname+'?multipart-manifest=put')
		obj_curl.setopt(pycurl.HTTPHEADER, headlist)
		obj_curl.setopt(pycurl.CUSTOMREQUEST, "PUT")
		obj_curl.setopt(pycurl.POST, 1)
		obj_curl.setopt(pycurl.POSTFIELDS, data)
		obj_curl.setopt(pycurl.WRITEFUNCTION, obj_response.write)
		obj_curl.perform()
		http_status = obj_curl.getinfo(pycurl.HTTP_CODE)
		obj_curl.close()


		if http_status == 400 :
			self.Log("ERROR : Cannot create manifest file for object %s in container %s" % (objectname, containername))
			return False
		elif http_status == 200 or http_status == 201 :
			self.Log("Create manifest file for object %s in container %s" % (objectname, containername))
			return True
		


	def getSLOManifest(self, containername, objectname) :
		if self.isprotected :
			return

		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		headlist = ['X-Auth-Token:'+self.authtoken]

		obj_response = StringIO()
		obj_curl = pycurl.Curl()
		obj_curl.setopt(pycurl.URL,self.swifturl+self.account+'/'+containername+'/'+objectname+'?multipart-manifest=get')
		obj_curl.setopt(pycurl.HTTPHEADER, headlist)
		obj_curl.setopt(pycurl.WRITEFUNCTION, obj_response.write)
		obj_curl.perform()
		http_status = obj_curl.getinfo(pycurl.HTTP_CODE)
		obj_curl.close()

		if http_status == 400 :
			self.Log("ERROR : Manifest for object % in container %s not found" % (objectname, containername))
			return False
		elif http_status == 200 or http_status == 201 :
			return obj_response.getvalue()
		else :
			self.Log("ERROR : Cannot create manifest for object %s in container %s" % (objectname, containername))
			return False
		


	def putDLOManifest(self, containername, objectname, manifest) :
		if self.isprotected :
			return

		status = True

		if not self.authtoken :
			status = self.getToken()

		if not status :
			return status

		headlist = ['X-Auth-Token:'+self.authtoken, 'X-Object-Manifest:'+manifest,'Content-Length:0']

		obj_response = StringIO()
		obj_curl = pycurl.Curl()
		obj_curl.setopt(pycurl.URL,self.swifturl+self.account+'/'+containername+'/'+objectname)
		obj_curl.setopt(pycurl.HTTPHEADER, headlist)
		obj_curl.setopt(pycurl.CUSTOMREQUEST, "PUT")
		obj_curl.setopt(pycurl.HEADERFUNCTION, obj_response.write)
		obj_curl.perform()
		http_status = obj_curl.getinfo(pycurl.HTTP_CODE)
		obj_curl.close()

		if http_status == 404 :
			self.Log("ERROR : Cannot create manifest for object %s in container %s" % (objectname, containername))
			return False
		elif http_status == 200 or http_status == 201 :
			self.Log("Create manifest for object %s in container %s" % (objectname, containername))
			return True
		else :
			self.Log("ERROR : Cannot create manifest for object %s in container %s" % (objectname, containername))
			return False
		


	def defaultLog(self, msg) :
		print msg



	def defaultUpdateProgress(self, success, fail) :
		self.success = self.success + success
		self.fail = self.fail + fail
		self.progress = self.success + self.fail

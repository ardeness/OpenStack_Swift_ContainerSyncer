import time
import json
import signal
import collections

from containermanager import ContainerManager

class ContainerSyncer(object) :

	def __init__(self) :
		self.srccontainer = None
		self.destcontainer = None
		self.Log = self.defaultLog
		self.alarmqueue = []
		self.handlerqueue = []
		self.isalarmset = False
		self.nextwaketime = 9999999999
		self.updatelength = 0
		self.deletelength = 0
		self.totallength = 0
		self.success = 0
		self.failed = 0
		self.current = 0
		self.updatecontainerlist = []
		self.deletecontainerlist = []
		self.updatelist = {} 
		self.deletelist = {}
		self.success = 0
		self.fail = 0
		self.progress = 0
		self.logfile = None
		self.notifyfile = None



	def nullLog(self, msg) :
		return



	def setAlarm(self, alarmtime) :
		if alarmtime < self.nextwaketime :
			self.nextwaketime = alarmtime
			currenttime = int(time.time())
			signal.signal(signal.SIGALRM, self.alarmHandler)
			waketime = self.nextwaketime - currenttime
			if waketime < 1 :
				waketime = 10

			signal.alarm(waketime)



	def alarmHandler(self, signum, frame) :
		self.nextwaketime = 9999999999
		self.srccontainer.getToken()
		self.destcontainer.getToken()
		



	def setSrcContainer(self, keystoneurl, swifturl, account, tenant, username, password, authmethod="keystone") :
		self.srccontainer = ContainerManager( keystoneurl = keystoneurl, \
						      swifturl = swifturl, \
						      account = account, \
						      tenant = tenant, \
						      username = username, \
						      password = password, \
						      authmethod = authmethod, \
						      isprotected = True )
		#self.srccontainer.Log = self.nullLog
		self.srccontainer.Log = self.defaultLog
		self.srccontainer.setAlarm = self.setAlarm
		self.srccontainer.updateProgress = self.updateProgress



	def setDestContainer(self, keystoneurl, swifturl, account, tenant, username, password, authmethod="keystone") :
		self.destcontainer = ContainerManager( keystoneurl = keystoneurl, \
						      swifturl = swifturl, \
						      account = account, \
						      tenant = tenant, \
						      username = username, \
						      password = password, \
						      authmethod=authmethod, \
						      isprotected = False )
		#self.destcontainer.Log = self.nullLog
		self.destcontainer.Log = self.defaultLog
		self.destcontainer.setAlarm = self.setAlarm
		self.destcontainer.updateProgress = self.updateProgress



	def syncAllContainer(self, maxdeleteratio=0.5) :
		self.Log("Syncing all container")
		if not self.srccontainer :
			self.Log("Source site does not exists")
			return False

		if not self.destcontainer :
			self.Log("Destination site does not exists")
			return False

		srccontainerlist = self.srccontainer.getContainerList()
		if srccontainerlist == False :
			self.Log("Get container list for source site failed")
			return False

		destcontainerlist = self.destcontainer.getContainerList()
		if destcontainerlist == False :
			self.Log("Get container list for destination site failed")
			return False

		self.Notify("Preparing for syncing all containers")

		status = self.prepareProcess(maxdeleteratio)

		if not status :
			self.Notify("Abort syncing all container.")
			return status


		# Create or update container
		for containername in self.updatecontainerlist :
			self.Log("Staring syncing container %s" % containername)
			status = self.syncContainerwithList(containername, containername, self.updatelist[containername], self.deletelist[containername])
			if not status :
				self.Log("Syncing %s container failed" % containername)
			else :
				self.Log("Container %s sync completed" % containername)
			

		# Delete container
		for containername in self.deletecontainerlist :
			status = self.destcontainer.deleteContainer(containername)

			if not status :
				self.Log("Deleting container %s in destination site failed" % containername)
				#return status
			else :
				self.Log("Container %s in destination site deleted" % containername)

		self.Notify("Syncing all containers completed - Total : %s, Success : %s, Fail : %s" \
				% (str(self.totallength), str(self.success), str(self.fail)))

		return True
	



	def syncContainer(self, srcname, destname) :

		self.Log("Syncing container %s and %s" % (srcname, destname))
		if not self.srccontainer :
			self.Log("Source site does not exists")
			return False

		if not self.destcontainer :
			self.Log("Destination site does not exists")
			return False

		srcobjectlist = self.srccontainer.getObjectList(srcname)
		if srcobjectlist == False:
			self.Log("Get object list from source site container %s failed" % srcname)
			return False

		destobjectlist = self.destcontainer.getObjectList(destname)
		if destobjectlist == False:
			self.Log("Get object list from destination site container %s failed" % destname)
			return False

		# Create or update objects in destination container
		updatelist = {}

		for objectname, metadata in srcobjectlist.items() :	
			if not 	objectname in destobjectlist or \
				destobjectlist[objectname]['lastmodifiedtime'] < metadata['lastmodifiedtime']:
				updatelist[objectname]=srcobjectlist[objectname]['type']

		updatelist = collections.OrderedDict(sorted(updatelist.items()))

		length = len(updatelist)
		completed = 0
		successd = 0
		failed = 0
		logmsg = ''

		self.Log("Updating %s objects in container %s" % (str(length), destname))
		for updatetarget, objtype in updatelist.items() :
			if objtype == 'DLO' :
				status = self.syncDLObject(srcname, destname, updatetarget)
			elif objtype == 'SLO' :
				status = self.syncSLObject(srcname, destname, updatetarget)
			else :
				contents = self.srccontainer.getObject(srcname, updatetarget)
				status = self.destcontainer.putObject(destname, updatetarget, contents)
				contents.close()

			if status :
				successed = successed + 1
				self.updateProgress(1,0)
				logmsg = "updated"
			else      :
				failed = failed + 1
				self.updateProgress(0,1)
				logmsg = "not updated"
			completed = completed + 1
			self.Log("Object %s %s - Success : %s, Failed : %s, Total : %s" \
				  % ( updatetarget, logmsg, str(successed), str(failed), str(completed)))

		# Delete objects from destination container
		deletelist = []

		for objectname, metadata in destobjectlist.items() :
			if objectname not in srcobjectlist :
				deletelist.append(objectname)

		deletelist = sorted(deletelist, reverse=True)

		length = len(deletelist)
		successed = 0
		failed = 0
		completed = 0

		self.Log("Deleting %s files in container %s" % (str(length), destname))
		for deletetarget in deletelist :
			status = self.destcontainer.deleteObject(destname, deletetarget)
			if status :
				successed = successed + 1
				self.updateProgress(1,0)
				logmsg = "deleted"
			else      :
				failed = failed + 1
				self.updateProgress(0,1)
				logmsg = "not deleted"
			completed = completed = 1

			self.Log("Object %s %s - Success : %s, Failed : %s, Total : %s" \
				  % ( deletetarget, logmsg, str(successed), str(failed), str(completed)))

		return True



	def syncContainerwithList(self, srcname, destname, updatelist, deletelist) :

		#self.Log("Syncing container %s and %s" % (srcname, destname))
		if not self.srccontainer :
			self.Log("Source site does not exists")
			return False

		if not self.destcontainer :
			self.Log("Destination site does not exists")
			return False

		# Create or update objects in destination container
		updatelist = collections.OrderedDict(sorted(updatelist.items()))

		length = len(updatelist)
		completed = 0
		successed = 0
		failed = 0
		logmsg = '' 

		self.Log("Updating %s objects in container %s" % (str(length), destname))
		for updatetarget, objtype in updatelist.items() :
			if objtype == 'DLO' :
				status = self.syncDLObject(srcname, destname, updatetarget)
			elif objtype == 'SLO' :
				status = self.syncSLObject(srcname, destname, updatetarget)
			else :
				contents = self.srccontainer.getObject(srcname, updatetarget)
				status = self.destcontainer.putObject(destname, updatetarget, contents)
				contents.close()

			if status :
				successed = successed + 1
				self.updateProgress(1, 0)
				logmsg = "updated"
			else      :
				failed = failed + 1
				self.updateProgress(0, 1)
				logmsg = "not updated"
			completed = completed + 1
			self.Log("Object %s %s - Success : %s, Failed : %s, Total : %s" \
				  % ( updatetarget, logmsg, str(successed), str(failed), str(completed)))

		# Delete objects from destination container
		deletelist = sorted(deletelist, reverse=True)

		length = len(deletelist)
		successed = 0
		failed = 0
		completed = 0

		self.Log("Deleting %s files in container %s" % (str(length), destname))
		for deletetarget in deletelist :
			status = self.destcontainer.deleteObject(destname, deletetarget)
			if status :
				successed = successed + 1
				self.updateProgress(1, 0)
				logmsg = "deleted"
			else      :
				failed = failed + 1
				self.updateProgress(0, 1)
				logmsg = "not deleted"
			completed = completed = 1

			self.Log("Object %s %s - Success : %s, Failed : %s, Total : %s" \
				  % ( deletetarget, logmsg, str(successed), str(failed), str(completed)))

		return True



	def syncSLObject(self, srcname, destname, objectname) :

		self.Log("Syncing SLObject %s in container %s" % (objectname, srcname))
		manifestdata = self.srccontainer.getSLOManifest(srcname, objectname)
		manifest=[]

		seglist = json.load(StringIO(manifestdata))

		for segment in seglist :
			objpath = '/'.join(segment['name'].split('/')[1:])

			self.Log("Uploading " + objpath)

			etag = segment['hash']
			size = int(segment['bytes'])
			info = {}
			info['path']=containername+'/'+objpath
			info['size_bytes']=size
			info['etag'] = etag

			manifest.append(info)

			contents = self.srccontainer.getObject(srcname, objpath)
			status = self.destcontainer.putObject(destname, objpath, contents)
			contents.close()

			if not status :
				self.Log("Uploading segment files for object %s failed. abort" % objectname)
				return False

			seqno = seqno + 1

		return self.destcontainer.putSLOManifest(destname, objectname, manifest) 



	def syncDLObject(self, srcname, destname, objectname) :

		self.Log("Syncing DLObject %s in container %s" % (objectname, srcname))
		objmeta = self.srccontainer.getObjectMetadata(srcname, objectname)
		srcpath = objmeta['X-Object-Manifest']
		segpath = '/'.join(srcpath.split('/')[1:])
		seqno = 0
		manifest=[]

		while True :
			objpath = segpath+str(seqno).zfill(8)
			objmeta = self.srccontainer.getObjectMetadata(srcname, objpath)
			if objmeta == False :
				break

			self.Log("Uploading " + objpath)

			contents = self.srccontainer.getObject(srcname, objpath)
			status = self.destcontainer.putObject(destname, objpath, contents)
			contents.close()

			if not status :
				self.Log("Uploading segment files for object %s failed. abort" % objectname)
				return False

			seqno = seqno + 1

		return self.destcontainer.putDLOManifest(destname, objectname, srcpath) 



	def prepareProcess(self,maxdeleteratio=0.5) :

		destdelobjcount=0
		desttotalobjcount=0

		if not self.srccontainer :
			self.Log("Source site does not exists")
			return False

		if not self.destcontainer :
			self.Log("Destination site does not exists")
			return False

		srccontainerlist = self.srccontainer.getContainerList()
		if srccontainerlist == False :
			self.Log("Failed to get source site container list")
			return False

		destcontainerlist = self.destcontainer.getContainerList()
		if destcontainerlist == False:
			self.Log("Failed to get destination site container list")
			return False

		# Get total object count on destination site
		for containername in destcontainerlist :
			count = self.destcontainer.getObjectCount(containername)
			if count :
				desttotalobjcount = desttotalobjcount + int(count)

		# Create or update container
		for containername in srccontainerlist :
			self.updatecontainerlist.append(containername)
			if containername not in destcontainerlist :
				self.Log("Container %s does not exists in destination site. Create %s"%(containername,containername))
				status = self.destcontainer.createContainer(containername)

				if not status :
					self.Log("Creating container %s in destination site failed. Abort" % containername)
					return status
				else :
					self.Log("Container %s in destination site created" % containername)

		# Delete container
		for containername in destcontainerlist :
			if containername not in srccontainerlist :
				self.deletecontainerlist.append(containername)
				count = self.destcontainer.getObjectCount(containername)
				if count :
					destdelobjcount = destdelobjcount + int(count)
					self.totallength = self.totallength + count


		# Parse update object list
		for containername in self.updatecontainerlist :
			srcobjectlist = self.srccontainer.getObjectList(containername)
			if srcobjectlist == False:
				self.Log("Get object list from source site container %s failed" % containername)
				continue

			destobjectlist = self.destcontainer.getObjectList(containername)
			if destobjectlist == False:
				self.Log("Get object list from destination site container %s failed" % containername)
				continue

			# Delete objects list from destination container
			deletelist = []

			for objectname, metadata in destobjectlist.items() :
				if objectname not in srcobjectlist :
					deletelist.append(objectname)

			deletelist = sorted(deletelist, reverse=True)

			# Create or update objects list in destination container
			updatelist = {}

			for objectname, metadata in srcobjectlist.items() :	
				if not 	objectname in destobjectlist or \
					destobjectlist[objectname]['lastmodifiedtime'] < metadata['lastmodifiedtime']:
					updatelist[objectname]=srcobjectlist[objectname]['type']

			updatelist = collections.OrderedDict(sorted(updatelist.items()))

			self.updatelength = self.updatelength + len(updatelist)
			self.deletelength = self.deletelength + len(deletelist)
			self.totallength = self.totallength + len(updatelist) + len(deletelist)
			self.updatelist[containername] = updatelist
			self.deletelist[containername] = deletelist

			destdelobjcount = destdelobjcount + len(deletelist)


		# Abort sync if delete ratio exceed threshold value
		ratio = float(destdelobjcount)/float(desttotalobjcount)
		
		if ratio > maxdeleteratio :
			self.Log("Abort : Number of deleting objects exceed allowed ratio : %s" % maxdeleteratio)
			return False

		return True



	def updateProgress(self, success, fail) :
		self.success = self.success + success
		self.fail = self.fail + fail
		self.progress = self.success + self.fail



	def getProgressMsg(self) :
		return "( %s / %s )" % (str(self.progress), str(self.totallength))
		


	def defaultLog(self, msg) :
		if self.logfile == None :
			today = time.strftime("%Y%m%d")
			self.logfile = open("/var/log/swiftDR/log_"+today, 'a')

		timestamp = time.strftime("%a, %d %b %Y %H:%M:%S %Z")
		self.logfile.write(timestamp + " " + msg + " " + self.getProgressMsg() + '\n')


	def Notify(self, msg) :
		if self.notifyfile == None :
			today = time.strftime("%Y%m%d")
			#self.notifyfile = open("/var/log/swiftDR/notify_"+today, 'a')
			self.notifyfile = open("/var/log/swiftDR/notify", 'a')

		timestamp = time.strftime("%a, %d %b %Y %H:%M:%S %Z")
		self.notifyfile.write(timestamp + " " + msg + '\n')

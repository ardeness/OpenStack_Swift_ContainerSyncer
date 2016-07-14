from containersyncer import ContainerSyncer

srckeystoneurl = "http://10.12.17.21:5000/v2.0/tokens/"
srcswifturl = "http://10.12.0.23:8080/v1/"
srcaccount="AUTH_00f55393e77246dca5e4a3a144225989"
srctenant = "tenantname"
srcusername = "username"
srcpassword = "password"

destkeystoneurl = "http://10.12.17.21:8080/auth/v1.0/"
destswifturl = "http://10.12.0.23:8080/v1/"
destaccount="AUTH_admin"
desttenant = "tenantname"
destusername = "usergroup:username"
destpassword = "password"

cs = ContainerSyncer()
cs.setSrcContainer(srckeystoneurl, srcswifturl, srcaccount, srctenant, srcusername, srcpassword)

# If swift proxy use temp_auth, set authmethod to "tempauth".
# If not specified, it use "keystone" by default.
#cs.setDestContainer(destkeystoneurl, swifturl, account, tenant, username, password, authmethod="tempauth")
cs.setDestContainer(destkeystoneurl, destswifturl, destaccount, desttenant, destusername, destpassword)

cs.syncAllContainer(0.9)

# We set default maximum allowed deletion ratio to 0.5. If you want to raise the ratio, 
# pass it as parameter like this :
#
# cs.syncAllContainer(maxdeleteratio=0.8)
# 
# It you set the "maxdeleteratio" value to 1.0, it will ignore the ratio value.

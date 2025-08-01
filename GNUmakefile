# sassrv makefile
lsb_dist     := $(shell if [ -f /etc/os-release ] ; then \
                  grep '^NAME=' /etc/os-release | sed 's/.*=[\"]*//' | sed 's/[ \"].*//' ; \
                  elif [ -x /usr/bin/lsb_release ] ; then \
                  lsb_release -is ; else echo Linux ; fi)
lsb_dist_ver := $(shell if [ -f /etc/os-release ] ; then \
		  grep '^VERSION=' /etc/os-release | sed 's/.*=[\"]*//' | sed 's/[ \"].*//' ; \
                  elif [ -x /usr/bin/lsb_release ] ; then \
                  lsb_release -rs | sed 's/[.].*//' ; else uname -r | sed 's/[-].*//' ; fi)
#lsb_dist     := $(shell if [ -x /usr/bin/lsb_release ] ; then lsb_release -is ; else echo Linux ; fi)
#lsb_dist_ver := $(shell if [ -x /usr/bin/lsb_release ] ; then lsb_release -rs | sed 's/[.].*//' ; else uname -r | sed 's/[-].*//' ; fi)
uname_m      := $(shell uname -m)

short_dist_lc := $(patsubst CentOS,rh,$(patsubst RedHatEnterprise,rh,\
                   $(patsubst RedHat,rh,\
                     $(patsubst Fedora,fc,$(patsubst Ubuntu,ub,\
                       $(patsubst Debian,deb,$(patsubst SUSE,ss,$(lsb_dist))))))))
short_dist    := $(shell echo $(short_dist_lc) | tr a-z A-Z)
pwd           := $(shell pwd)
rpm_os        := $(short_dist_lc)$(lsb_dist_ver).$(uname_m)

# this is where the targets are compiled
build_dir ?= $(short_dist)$(lsb_dist_ver)_$(uname_m)$(port_extra)
bind      := $(build_dir)/bin
libd      := $(build_dir)/lib64
objd      := $(build_dir)/obj
dependd   := $(build_dir)/dep

have_rpm  := $(shell if [ -x /bin/rpmquery ] ; then echo true; fi)
have_dpkg := $(shell if [ -x /bin/dpkg-buildflags ] ; then echo true; fi)
default_cflags := -ggdb -O3
# use 'make port_extra=-g' for debug build
ifeq (-g,$(findstring -g,$(port_extra)))
  default_cflags := -ggdb
endif
ifeq (-a,$(findstring -a,$(port_extra)))
  default_cflags += -fsanitize=address
endif
ifeq (-mingw,$(findstring -mingw,$(port_extra)))
  CC    := /usr/bin/x86_64-w64-mingw32-gcc
  CXX   := /usr/bin/x86_64-w64-mingw32-g++
  mingw := true
endif
ifeq (,$(port_extra))
  ifeq (true,$(have_rpm))
    build_cflags = $(shell /bin/rpm --eval '%{optflags}')
  endif
  ifeq (true,$(have_dpkg))
    build_cflags = $(shell /bin/dpkg-buildflags --get CFLAGS)
  endif
endif
# msys2 using ucrt64
ifeq (MSYS2,$(lsb_dist))
  mingw := true
endif
CC          ?= gcc
CXX         ?= g++
cc          := $(CC) -std=c11
cpp         := $(CXX)
arch_cflags := -mavx -maes -fno-omit-frame-pointer
gcc_wflags  := -Wall -Wextra
#-Werror
# if windows cross compile
ifeq (true,$(mingw))
dll         := dll
exe         := .exe
soflag      := -shared -Wl,--subsystem,windows
fpicflags   := -fPIC -DRV_SHARED
sock_lib    := -lcares -lws2_32
dynlink_lib := -lpcre2-8 -lz
NO_STL      := 1
else
dll         := so
exe         :=
soflag      := -shared
fpicflags   := -fPIC
thread_lib  := -pthread -lrt
sock_lib    := -lcares
dynlink_lib := -lpcre2-8 -lz
endif
# make apple shared lib
ifeq (Darwin,$(lsb_dist)) 
dll         := dylib
endif
# rpmbuild uses RPM_OPT_FLAGS
#ifeq ($(RPM_OPT_FLAGS),)
CFLAGS ?= $(build_cflags) $(default_cflags)
#else
#CFLAGS ?= $(RPM_OPT_FLAGS)
#endif
cflags := $(gcc_wflags) $(CFLAGS) $(arch_cflags)
lflags := -Wno-stringop-overflow

INCLUDES  ?= -Iinclude
DEFINES   ?=
includes  := $(INCLUDES)
defines   := $(DEFINES)

# if not linking libstdc++
ifdef NO_STL
cppflags  := -std=c++11 -fno-rtti -fno-exceptions
cpplink   := $(CC)
else
cppflags  := -std=c++11
cpplink   := $(CXX)
endif

math_lib    := -lm

# test submodules exist (they don't exist for dist_rpm, dist_dpkg targets)
test_makefile = $(shell if [ -f ./$(1)/GNUmakefile ] ; then echo ./$(1) ; \
                        elif [ -f ../$(1)/GNUmakefile ] ; then echo ../$(1) ; fi)

md_home     := $(call test_makefile,raimd)
dec_home    := $(call test_makefile,libdecnumber)
kv_home     := $(call test_makefile,raikv)

ifeq (,$(dec_home))
dec_home    := $(call test_makefile,$(md_home)/libdecnumber)
endif

lnk_lib     := -Wl,--push-state -Wl,-Bstatic
dlnk_lib    :=
lnk_dep     :=
dlnk_dep    :=
rv_dlnk_lib := -L$(pwd)/$(libd) -lsassrv
rv_dlnk_dep := $(libd)/libsassrv.$(dll)

ifneq (,$(md_home))
md_lib      := $(md_home)/$(libd)/libraimd.a
md_dll      := $(md_home)/$(libd)/libraimd.$(dll)
lnk_lib     += $(md_lib)
lnk_dep     += $(md_lib)
dlnk_lib    += -L$(md_home)/$(libd) -lraimd
dlnk_dep    += $(md_dll)
rpath1       = ,-rpath,$(pwd)/$(md_home)/$(libd)
includes    += -I$(md_home)/include
else
lnk_lib     += $(push_static) -lraimd $(pop_static)
dlnk_lib    += -lraimd
endif

ifneq (,$(dec_home))
dec_lib     := $(dec_home)/$(libd)/libdecnumber.a
dec_dll     := $(dec_home)/$(libd)/libdecnumber.$(dll)
lnk_lib     += $(dec_lib)
lnk_dep     += $(dec_lib)
dlnk_lib    += -L$(dec_home)/$(libd) -ldecnumber
dlnk_dep    += $(dec_dll)
rpath2       = ,-rpath,$(pwd)/$(dec_home)/$(libd)
dec_includes = -I$(dec_home)/include
else
lnk_lib     += $(push_static) -ldecnumber $(pop_static)
dlnk_lib    += -ldecnumber
endif

ifneq (,$(kv_home))
kv_lib      := $(kv_home)/$(libd)/libraikv.a
kv_dll      := $(kv_home)/$(libd)/libraikv.$(dll)
lnk_lib     += $(kv_lib)
lnk_dep     += $(kv_lib)
dlnk_lib    += -L$(kv_home)/$(libd) -lraikv
dlnk_dep    += $(kv_dll)
rpath3       = ,-rpath,$(pwd)/$(kv_home)/$(libd)
includes    += -I$(kv_home)/include
else
lnk_lib     += $(push_static) -lraikv $(pop_static)
dlnk_lib    += -lraikv
endif

rpath   := -Wl,-rpath,$(pwd)/$(libd)$(rpath1)$(rpath2)$(rpath3)
lnk_lib += -Wl,--pop-state

.PHONY: everything
everything: $(kv_lib) $(dec_lib) $(md_lib) all

clean_subs :=
# build submodules if have them
ifneq (,$(kv_home))
$(kv_lib) $(kv_dll):
	$(MAKE) -C $(kv_home)
.PHONY: clean_kv
clean_kv:
	$(MAKE) -C $(kv_home) clean
clean_subs += clean_kv
endif
ifneq (,$(dec_home))
$(dec_lib) $(dec_dll):
	$(MAKE) -C $(dec_home)
.PHONY: clean_dec
clean_dec:
	$(MAKE) -C $(dec_home) clean
clean_subs += clean_dec
endif
ifneq (,$(md_home))
$(md_lib) $(md_dll):
	$(MAKE) -C $(md_home)
.PHONY: clean_md
clean_md:
	$(MAKE) -C $(md_home) clean
clean_subs += clean_md
endif

# copr/fedora build (with version env vars)
# copr uses this to generate a source rpm with the srpm target
-include .copr/Makefile

# debian build (debuild)
# target for building installable deb: dist_dpkg
-include deb/Makefile

# targets filled in below
all_exes    :=
all_libs    :=
all_dlls    :=
all_depends :=
gen_files   :=

ev_rv_defines  := -DSASSRV_VER=$(ver_build)
$(objd)/ev_rv.o : .copr/Makefile
$(objd)/ev_rv.fpic.o : .copr/Makefile
libsassrv_files := ev_rv rv_host ev_rv_client submgr ft mc
libsassrv_cfile := $(addprefix src/, $(addsuffix .cpp, $(libsassrv_files)))
libsassrv_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(libsassrv_files)))
libsassrv_dbjs  := $(addprefix $(objd)/, $(addsuffix .fpic.o, $(libsassrv_files)))
libsassrv_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(libsassrv_files))) \
                  $(addprefix $(dependd)/, $(addsuffix .fpic.d, $(libsassrv_files)))
libsassrv_dlnk  := $(dlnk_lib)
libsassrv_spec  := $(version)-$(build_num)_$(git_hash)
libsassrv_ver   := $(major_num).$(minor_num)

$(libd)/libsassrv.a: $(libsassrv_objs)
$(libd)/libsassrv.$(dll): $(libsassrv_dbjs) $(dlnk_dep)

all_libs    += $(libd)/libsassrv.a
all_dlls    += $(libd)/libsassrv.$(dll)
all_depends += $(libsassrv_deps)
sassrv_lib  := $(libd)/libsassrv.a

server_defines := -DSASSRV_VER=$(ver_build)
$(objd)/server.o : .copr/Makefile
$(objd)/server.fpic.o : .copr/Makefile
rv_server_files := server
rv_server_cfile := $(addprefix src/, $(addsuffix .cpp, $(rv_server_files)))
rv_server_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(rv_server_files)))
rv_server_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(rv_server_files)))
rv_server_libs  := $(sassrv_lib)
rv_server_lnk   := $(sassrv_lib) $(lnk_lib)

$(bind)/rv_server$(exe): $(rv_server_objs) $(rv_server_libs) $(lnk_dep)

all_exes    += $(bind)/rv_server$(exe)
all_depends += $(rv_server_deps)

rv_client_files := client
rv_client_cfile := $(addprefix src/, $(addsuffix .cpp, $(rv_client_files)))
rv_client_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(rv_client_files)))
rv_client_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(rv_client_files)))
rv_client_libs  := $(sassrv_lib)
rv_client_lnk   := $(sassrv_lib) $(lnk_lib)

$(bind)/rv_client$(exe): $(rv_client_objs) $(rv_client_libs) $(lnk_dep)

all_exes    += $(bind)/rv_client$(exe)
all_depends += $(rv_client_deps)

rv_pub_files := pub
rv_pub_cfile := $(addprefix src/, $(addsuffix .cpp, $(rv_pub_files)))
rv_pub_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(rv_pub_files)))
rv_pub_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(rv_pub_files)))
rv_pub_libs  := $(sassrv_lib)
rv_pub_lnk   := $(sassrv_lib) $(lnk_lib)
#rv_pub_libs  :=
#rv_pub_lnk   := $(rv_dlnk_lib) $(dlnk_lib)

#$(bind)/rv_pub$(exe): $(rv_pub_objs) $(rv_pub_libs) $(dlnk_dep) $(rv_dlnk_dep)

$(bind)/rv_pub$(exe): $(rv_pub_objs) $(rv_pub_libs) $(lnk_dep)

all_exes    += $(bind)/rv_pub$(exe)
all_depends += $(rv_pub_deps)

#tibco_home = $(shell if [ -d /usr/tibco/tibrv ] ; then echo /usr/tibco/tibrv ; \
#                        elif [ -d /home/chris/tibco/tibrv ] ; then echo /home/chris/tibco/tibrv ; fi)
#api_client_includes = -I$(tibco_home)/include
api_client_files := api_client
api_client_cfile := $(addprefix src/, $(addsuffix .cpp, $(api_client_files)))
api_client_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(api_client_files)))
api_client_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(api_client_files)))
api_client_libs  := $(sassrv_lib) $(libd)/librv7lib.a
#api_client_lnk   := $(lnk_lib) $(tibco_home)/lib/librv7lib64.a
api_client_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)

$(bind)/api_client$(exe): $(api_client_objs) $(api_client_libs) $(lnk_dep)

all_exes    += $(bind)/api_client$(exe)
all_depends += $(api_client_deps)

rv_subtop_files := subtop
rv_subtop_cfile := $(addprefix src/, $(addsuffix .cpp, $(rv_subtop_files)))
rv_subtop_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(rv_subtop_files)))
rv_subtop_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(rv_subtop_files)))
rv_subtop_libs  := $(sassrv_lib)
rv_subtop_lnk   := $(sassrv_lib) $(lnk_lib)

$(bind)/rv_subtop$(exe): $(rv_subtop_objs) $(rv_subtop_libs) $(lnk_dep)

all_exes    += $(bind)/rv_subtop$(exe)
all_depends += $(rv_subtop_deps)

rv_ftmon_files := ftmon
rv_ftmon_cfile := $(addprefix src/, $(addsuffix .cpp, $(rv_ftmon_files)))
rv_ftmon_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(rv_ftmon_files)))
rv_ftmon_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(rv_ftmon_files)))
rv_ftmon_libs  := $(sassrv_lib)
rv_ftmon_lnk   := $(sassrv_lib) $(lnk_lib)

$(bind)/rv_ftmon$(exe): $(rv_ftmon_objs) $(rv_ftmon_libs) $(lnk_dep)

all_exes    += $(bind)/rv_ftmon$(exe)
all_depends += $(rv_ftmon_deps)

rv5_api_defines := -DSASSRV_VER=$(ver_build)
librv5lib_files := rv5_api
librv5lib_cfile := $(addprefix src/, $(addsuffix .cpp, $(librv5lib_files)))
librv5lib_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(librv5lib_files)))
librv5lib_dbjs  := $(addprefix $(objd)/, $(addsuffix .fpic.o, $(librv5lib_files)))
librv5lib_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(librv5lib_files))) \
                  $(addprefix $(dependd)/, $(addsuffix .fpic.d, $(librv5lib_files)))
librv5lib_dlnk  := $(rv_dlnk_lib) $(dlnk_lib)
librv5lib_spec  := $(version)-$(build_num)_$(git_hash)
librv5lib_ver   := $(major_num).$(minor_num)

$(libd)/librv5lib.a: $(librv5lib_objs)
$(libd)/librv5lib.$(dll): $(librv5lib_dbjs) $(rv_dlnk_dep) $(dlnk_dep)

all_libs    += $(libd)/librv5lib.a
all_dlls    += $(libd)/librv5lib.$(dll)
all_depends += $(librv5lib_deps)

rv7_api_defines := -DSASSRV_VER=$(ver_build)
librv7lib_files := rv7_api rv7_msg
librv7lib_cfile := $(addprefix src/, $(addsuffix .cpp, $(librv7lib_files)))
librv7lib_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(librv7lib_files)))
librv7lib_dbjs  := $(addprefix $(objd)/, $(addsuffix .fpic.o, $(librv7lib_files)))
librv7lib_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(librv7lib_files))) \
                  $(addprefix $(dependd)/, $(addsuffix .fpic.d, $(librv7lib_files)))
librv7lib_dlnk  := $(rv_dlnk_lib) $(dlnk_lib)
librv7lib_spec  := $(version)-$(build_num)_$(git_hash)
librv7lib_ver   := $(major_num).$(minor_num)

$(libd)/librv7lib.a: $(librv7lib_objs)
$(libd)/librv7lib.$(dll): $(librv7lib_dbjs) $(rv_dlnk_dep) $(dlnk_dep)

all_libs    += $(libd)/librv7lib.a
all_dlls    += $(libd)/librv7lib.$(dll)
all_depends += $(librv7lib_deps)

rv7_ft_defines := -DSASSRV_VER=$(ver_build)
librv7ftlib_files := rv7_ft
librv7ftlib_cfile := $(addprefix src/, $(addsuffix .cpp, $(librv7ftlib_files)))
librv7ftlib_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(librv7ftlib_files)))
librv7ftlib_dbjs  := $(addprefix $(objd)/, $(addsuffix .fpic.o, $(librv7ftlib_files)))
librv7ftlib_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(librv7ftlib_files))) \
                  $(addprefix $(dependd)/, $(addsuffix .fpic.d, $(librv7ftlib_files)))
librv7ftlib_dlnk  := $(rv_dlnk_lib) $(dlnk_lib)
librv7ftlib_spec  := $(version)-$(build_num)_$(git_hash)
librv7ftlib_ver   := $(major_num).$(minor_num)

$(libd)/librv7ftlib.a: $(librv7ftlib_objs)
$(libd)/librv7ftlib.$(dll): $(librv7ftlib_dbjs) $(rv_dlnk_dep) $(dlnk_dep)

all_libs    += $(libd)/librv7ftlib.a
all_dlls    += $(libd)/librv7ftlib.$(dll)
all_depends += $(librv7ftlib_deps)

rv5_api_test_includes = -Iinclude/sassrv
rv5_api_test_files := rv5_api_test
rv5_api_test_cfile := $(addprefix src/, $(addsuffix .cpp, $(rv5_api_test_files)))
rv5_api_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(rv5_api_test_files)))
rv5_api_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(rv5_api_test_files)))
rv5_api_test_libs  := $(sassrv_lib) $(libd)/librv5lib.a
rv5_api_test_lnk   := $(libd)/librv5lib.a $(sassrv_lib) $(lnk_lib)

$(bind)/rv5_api_test$(exe): $(rv5_api_test_objs) $(rv5_api_test_libs) $(lnk_dep)

all_exes    += $(bind)/rv5_api_test$(exe)
all_depends += $(rv5_api_test_deps)

rv5_cpp_test_includes = -Iinclude/sassrv
rv5_cpp_test_files := rv5_cpp_test rv5_api
rv5_cpp_test_cfile := $(addprefix src/, $(addsuffix .cpp, $(rv5_cpp_test_files)))
rv5_cpp_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(rv5_cpp_test_files)))
rv5_cpp_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(rv5_cpp_test_files)))
rv5_cpp_test_libs  := $(sassrv_lib) $(libd)/librv5lib.a
rv5_cpp_test_lnk   := $(libd)/librv5lib.a $(sassrv_lib) $(lnk_lib)

$(bind)/rv5_cpp_test$(exe): $(rv5_cpp_test_objs) $(rv5_cpp_test_libs) $(lnk_dep)

all_exes    += $(bind)/rv5_cpp_test$(exe)
all_depends += $(rv5_cpp_test_deps)

# tibrvclient_files := tibrvclient
# tibrvclient_cfile := $(addprefix src/, $(addsuffix .cpp, $(tibrvclient_files)))
# tibrvclient_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(tibrvclient_files)))
# tibrvclient_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(tibrvclient_files)))
# tibrvclient_libs  := $(sassrv_lib) $(libd)/librv7lib.a
# tibrvclient_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)
#
# $(bind)/tibrvclient$(exe): $(tibrvclient_objs) $(tibrvclient_libs) $(lnk_dep)
#
# tibrvinitval_files := tibrvinitval
# tibrvinitval_cfile := $(addprefix src/, $(addsuffix .cpp, $(tibrvinitval_files)))
# tibrvinitval_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(tibrvinitval_files)))
# tibrvinitval_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(tibrvinitval_files)))
# tibrvinitval_libs  := $(sassrv_lib) $(libd)/librv7lib.a
# tibrvinitval_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)
#
# $(bind)/tibrvinitval$(exe): $(tibrvinitval_objs) $(tibrvinitval_libs) $(lnk_dep)
#
# tibrvlisten_files := tibrvlisten
# tibrvlisten_cfile := $(addprefix src/, $(addsuffix .cpp, $(tibrvlisten_files)))
# tibrvlisten_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(tibrvlisten_files)))
# tibrvlisten_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(tibrvlisten_files)))
# tibrvlisten_libs  := $(sassrv_lib) $(libd)/librv7lib.a
# tibrvlisten_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)
#
# $(bind)/tibrvlisten$(exe): $(tibrvlisten_objs) $(tibrvlisten_libs) $(lnk_dep)
#
# tibrvmultisend_files := tibrvmultisend
# tibrvmultisend_cfile := $(addprefix src/, $(addsuffix .cpp, $(tibrvmultisend_files)))
# tibrvmultisend_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(tibrvmultisend_files)))
# tibrvmultisend_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(tibrvmultisend_files)))
# tibrvmultisend_libs  := $(sassrv_lib) $(libd)/librv7lib.a
# tibrvmultisend_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)
#
# $(bind)/tibrvmultisend$(exe): $(tibrvmultisend_objs) $(tibrvmultisend_libs) $(lnk_dep)
#
# tibrvsend_files := tibrvsend
# tibrvsend_cfile := $(addprefix src/, $(addsuffix .cpp, $(tibrvsend_files)))
# tibrvsend_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(tibrvsend_files)))
# tibrvsend_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(tibrvsend_files)))
# tibrvsend_libs  := $(sassrv_lib) $(libd)/librv7lib.a
# tibrvsend_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)
#
# $(bind)/tibrvsend$(exe): $(tibrvsend_objs) $(tibrvsend_libs) $(lnk_dep)
#
# tibrvserver_files := tibrvserver
# tibrvserver_cfile := $(addprefix src/, $(addsuffix .cpp, $(tibrvserver_files)))
# tibrvserver_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(tibrvserver_files)))
# tibrvserver_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(tibrvserver_files)))
# tibrvserver_libs  := $(sassrv_lib) $(libd)/librv7lib.a
# tibrvserver_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)
#
# $(bind)/tibrvserver$(exe): $(tibrvserver_objs) $(tibrvserver_libs) $(lnk_dep)
#
# dispatcher_files := dispatcher
# dispatcher_cfile := $(addprefix src/, $(addsuffix .cpp, $(dispatcher_files)))
# dispatcher_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(dispatcher_files)))
# dispatcher_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(dispatcher_files)))
# dispatcher_libs  := $(sassrv_lib) $(libd)/librv7lib.a
# dispatcher_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)
#
# $(bind)/dispatcher$(exe): $(dispatcher_objs) $(dispatcher_libs) $(lnk_dep)
#
# tibrvvectorlisten_files := tibrvvectorlisten
# tibrvvectorlisten_cfile := $(addprefix src/, $(addsuffix .cpp, $(tibrvvectorlisten_files)))
# tibrvvectorlisten_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(tibrvvectorlisten_files)))
# tibrvvectorlisten_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(tibrvvectorlisten_files)))
# tibrvvectorlisten_libs  := $(sassrv_lib) $(libd)/librv7lib.a
# tibrvvectorlisten_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)
#
# $(bind)/tibrvvectorlisten$(exe): $(tibrvvectorlisten_objs) $(tibrvvectorlisten_libs) $(lnk_dep)
#
# tibrvvectorlistentester_files := tibrvvectorlistentester
# tibrvvectorlistentester_cfile := $(addprefix src/, $(addsuffix .cpp, $(tibrvvectorlistentester_files)))
# tibrvvectorlistentester_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(tibrvvectorlistentester_files)))
# tibrvvectorlistentester_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(tibrvvectorlistentester_files)))
# tibrvvectorlistentester_libs  := $(sassrv_lib) $(libd)/librv7lib.a
# tibrvvectorlistentester_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)
#
# $(bind)/tibrvvectorlistentester$(exe): $(tibrvvectorlistentester_objs) $(tibrvvectorlistentester_libs) $(lnk_dep)
#
# priority_files := priority
# priority_cfile := $(addprefix src/, $(addsuffix .cpp, $(priority_files)))
# priority_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(priority_files)))
# priority_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(priority_files)))
# priority_libs  := $(sassrv_lib) $(libd)/librv7lib.a
# priority_lnk   := $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)
#
# $(bind)/priority$(exe): $(priority_objs) $(priority_libs) $(lnk_dep)
#
#tibrvfttime_files := tibrvfttime
#tibrvfttime_cfile := $(addprefix src/, $(addsuffix .cpp, $(tibrvfttime_files)))
#tibrvfttime_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(tibrvfttime_files)))
#tibrvfttime_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(tibrvfttime_files)))
#tibrvfttime_libs  := $(sassrv_lib) $(libd)/librv7ftlib.$(dll) $(libd)/librv7lib.$(dll)
#tibrvfttime_lnk   := $(rv_dlnk_lib) -lrv7ftlib -lrv7lib $(dlnk_lib)
#
#$(bind)/tibrvfttime$(exe): $(tibrvfttime_objs) $(tibrvfttime_libs) $(lnk_dep)
#
#all_exes    += $(bind)/tibrvfttime$(exe)
#all_depends += $(tibrvfttime_deps)
#
#tibrvftmon_files := tibrvftmon
#tibrvftmon_cfile := $(addprefix src/, $(addsuffix .cpp, $(tibrvftmon_files)))
#tibrvftmon_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(tibrvftmon_files)))
#tibrvftmon_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(tibrvftmon_files)))
#tibrvftmon_libs  := $(sassrv_lib) $(libd)/librv7ftlib.$(dll) $(libd)/librv7lib.$(dll)
#tibrvftmon_lnk   := $(rv_dlnk_lib) -lrv7ftlib -lrv7lib $(dlnk_lib)
#
#$(bind)/tibrvftmon$(exe): $(tibrvftmon_objs) $(tibrvftmon_libs) $(lnk_dep)
#
#all_exes    += $(bind)/tibrvftmon$(exe)
#all_depends += $(tibrvftmon_deps)

rv7_test_files := rv7_test
rv7_test_cfile := $(addprefix src/, $(addsuffix .cpp, $(rv7_test_files)))
rv7_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(rv7_test_files)))
rv7_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(rv7_test_files)))
rv7_test_libs  := $(sassrv_lib) $(libd)/librv7ftlib.a $(libd)/librv7lib.a
rv7_test_lnk   := $(libd)/librv7ftlib.a $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)

$(bind)/rv7_test$(exe): $(rv7_test_objs) $(rv7_test_libs) $(lnk_dep)

all_exes    += $(bind)/rv7_test$(exe)
all_depends += $(rv7_test_deps)

intra_test_files := intraprocess
intra_test_cfile := $(addprefix src/, $(addsuffix .cpp, $(intra_test_files)))
intra_test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(intra_test_files)))
intra_test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(intra_test_files)))
intra_test_libs  := $(sassrv_lib) $(libd)/librv7ftlib.a $(libd)/librv7lib.a
intra_test_lnk   := $(libd)/librv7ftlib.a $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)

$(bind)/intra_test$(exe): $(intra_test_objs) $(intra_test_libs) $(lnk_dep)

all_exes    += $(bind)/intra_test$(exe)
all_depends += $(intra_test_deps)

subrv7test_files := subrv7test
subrv7test_cfile := $(addprefix src/, $(addsuffix .cpp, $(subrv7test_files)))
subrv7test_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(subrv7test_files)))
subrv7test_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(subrv7test_files)))
subrv7test_libs  := $(sassrv_lib) $(libd)/librv7ftlib.a $(libd)/librv7lib.a
subrv7test_lnk   := $(libd)/librv7ftlib.a $(libd)/librv7lib.a $(sassrv_lib) $(lnk_lib)

$(bind)/subrv7test$(exe): $(subrv7test_objs) $(subrv7test_libs) $(lnk_dep)

all_exes    += $(bind)/subrv7test$(exe)
all_depends += $(subrv7test_deps)

all_dirs := $(bind) $(libd) $(objd) $(dependd)

# the default targets
.PHONY: all
all: $(all_libs) $(all_dlls) $(all_exes) cmake

.PHONY: cmake
cmake: CMakeLists.txt

.ONESHELL: CMakeLists.txt
CMakeLists.txt: .copr/Makefile
	@cat <<'EOF' > $@
	cmake_minimum_required (VERSION 3.9.0)
	if (POLICY CMP0111)
	  cmake_policy(SET CMP0111 OLD)
	endif ()
	project (sassrv)
	include_directories (
	  include
	  $${CMAKE_SOURCE_DIR}/raimd/include
	  $${CMAKE_SOURCE_DIR}/raikv/include
	  $${CMAKE_SOURCE_DIR}/libdecnumber/include
	  $${CMAKE_SOURCE_DIR}/raimd/libdecnumber/include
	)
	if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
	  add_definitions(/DPCRE2_STATIC)
	  if ($$<CONFIG:Release>)
	    add_compile_options (/arch:AVX2 /GL /std:c11)
	  else ()
	    add_compile_options (/arch:AVX2 /std:c11)
	  endif ()
	  if (NOT TARGET pcre2-8-static)
	    add_library (pcre2-8-static STATIC IMPORTED)
	    set_property (TARGET pcre2-8-static PROPERTY IMPORTED_LOCATION_DEBUG ../pcre2/build/Debug/pcre2-8-staticd.lib)
	    set_property (TARGET pcre2-8-static PROPERTY IMPORTED_LOCATION_RELEASE ../pcre2/build/Release/pcre2-8-static.lib)
	    include_directories (../pcre2/build)
	  else ()
	    include_directories ($${CMAKE_BINARY_DIR}/pcre2)
	  endif ()
	  if (NOT TARGET raikv)
	    add_library (raikv STATIC IMPORTED)
	    set_property (TARGET raikv PROPERTY IMPORTED_LOCATION_DEBUG ../raikv/build/Debug/raikv.lib)
	    set_property (TARGET raikv PROPERTY IMPORTED_LOCATION_RELEASE ../raikv/build/Release/raikv.lib)
	  endif ()
	  if (NOT TARGET raimd)
	    add_library (raimd STATIC IMPORTED)
	    set_property (TARGET raimd PROPERTY IMPORTED_LOCATION_DEBUG ../raimd/build/Debug/raimd.lib)
	    set_property (TARGET raimd PROPERTY IMPORTED_LOCATION_RELEASE ../raimd/build/Release/raimd.lib)
	  endif ()
	  if (NOT TARGET decnumber)
	    add_library (decnumber STATIC IMPORTED)
	    set_property (TARGET decnumber PROPERTY IMPORTED_LOCATION_DEBUG ../raimd/libdecnumber/build/Debug/decnumber.lib)
	    set_property (TARGET decnumber PROPERTY IMPORTED_LOCATION_RELEASE ../raimd/libdecnumber/build/Release/decnumber.lib)
	  endif ()
	else ()
	  add_compile_options ($(cflags))
	  if (TARGET pcre2-8-static)
	    include_directories ($${CMAKE_BINARY_DIR}/pcre2)
	  endif ()
	  if (NOT TARGET raikv)
	    add_library (raikv STATIC IMPORTED)
	    set_property (TARGET raikv PROPERTY IMPORTED_LOCATION ../raikv/build/libraikv.a)
	  endif ()
	  if (NOT TARGET raimd)
	    add_library (raimd STATIC IMPORTED)
	    set_property (TARGET raimd PROPERTY IMPORTED_LOCATION ../raimd/build/libraimd.a)
	  endif ()
	  if (NOT TARGET decnumber)
	    add_library (decnumber STATIC IMPORTED)
	    set_property (TARGET decnumber PROPERTY IMPORTED_LOCATION ../raimd/libdecnumber/build/libdecnumber.a)
	  endif ()
	endif ()
	add_library (sassrv STATIC $(libsassrv_cfile))
	if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
	  link_libraries (sassrv raikv raimd decnumber pcre2-8-static ws2_32)
	else ()
	  if (TARGET pcre2-8-static)
	    link_libraries (sassrv raikv raimd decnumber pcre2-8-static -lcares -lpthread -lrt)
	  else ()
	    link_libraries (sassrv raikv raimd decnumber -lpcre2-8 -lcares -lpthread -lrt)
	  endif ()
	endif ()
	add_definitions(-DSASSRV_VER=$(ver_build))
	add_executable (rv_server $(rv_server_cfile))
	add_executable (rv_client $(rv_client_cfile))
	add_executable (rv_pub $(rv_pub_cfile))
	EOF

.PHONY: dnf_depend
dnf_depend:
	sudo dnf -y install make gcc-c++ git redhat-lsb openssl-devel pcre2-devel chrpath c-ares-devel

.PHONY: yum_depend
yum_depend:
	sudo yum -y install make gcc-c++ git redhat-lsb openssl-devel pcre2-devel chrpath c-ares-devel

.PHONY: deb_depend
deb_depend:
	sudo apt-get install -y install make g++ gcc devscripts libpcre2-dev chrpath git lsb-release libssl-dev c-ares-dev

# create directories
$(dependd):
	@mkdir -p $(all_dirs)

# remove target bins, objs, depends
.PHONY: clean
clean: $(clean_subs)
	rm -r -f $(bind) $(libd) $(objd) $(dependd)
	if [ "$(build_dir)" != "." ] ; then rmdir $(build_dir) ; fi

.PHONY: clean_dist
clean_dist:
	rm -rf dpkgbuild rpmbuild

.PHONY: clean_all
clean_all: clean clean_dist

# force a remake of depend using 'make -B depend'
.PHONY: depend
depend: $(dependd)/depend.make

$(dependd)/depend.make: $(dependd) $(all_depends)
	@echo "# depend file" > $(dependd)/depend.make
	@cat $(all_depends) >> $(dependd)/depend.make

.PHONY: dist_bins
dist_bins: $(all_libs) $(all_dlls) $(bind)/rv_server$(exe) $(bind)/rv_client$(exe) $(bind)/rv_pub$(exe) $(bind)/rv_subtop$(exe) $(bind)/rv_ftmon$(exe)
	chrpath -d $(libd)/libsassrv.$(dll)
	chrpath -d $(libd)/librv5lib.$(dll)
	chrpath -d $(libd)/librv7lib.$(dll)
	chrpath -d $(libd)/librv7ftlib.$(dll)
	chrpath -d $(bind)/rv_server$(exe)
	chrpath -d $(bind)/rv_client$(exe)
	chrpath -d $(bind)/rv_pub$(exe)
	chrpath -d $(bind)/rv_subtop$(exe)
	chrpath -d $(bind)/rv_ftmon$(exe)

.PHONY: dist_rpm
dist_rpm: srpm
	( cd rpmbuild && rpmbuild --define "-topdir `pwd`" -ba SPECS/sassrv.spec )

# dependencies made by 'make depend'
-include $(dependd)/depend.make

ifeq ($(DESTDIR),)
# 'sudo make install' puts things in /usr/local/lib, /usr/local/include
install_prefix = /usr/local
else
# debuild uses DESTDIR to put things into debian/sassrv/usr
install_prefix = $(DESTDIR)/usr
endif

install: dist_bins
	install -d $(install_prefix)/lib $(install_prefix)/bin
	install -d $(install_prefix)/include/sassrv
	for f in $(libd)/libsassrv.* $(libd)/librv5lib.* $(libd)/librv7lib.* ; do \
	if [ -h $$f ] ; then \
	cp -a $$f $(install_prefix)/lib ; \
	else \
	install $$f $(install_prefix)/lib ; \
	fi ; \
	done
	install -m 755 $(bind)/rv_server$(exe) $(install_prefix)/bin
	install -m 755 $(bind)/rv_client$(exe) $(install_prefix)/bin
	install -m 755 $(bind)/rv_pub$(exe) $(install_prefix)/bin
	install -m 755 $(bind)/rv_subtop$(exe) $(install_prefix)/bin
	install -m 755 $(bind)/rv_ftmon$(exe) $(install_prefix)/bin
	install -m 644 include/sassrv/*.h $(install_prefix)/include/sassrv

$(objd)/%.o: src/%.cpp
	$(cpp) $(cflags) $(cppflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: src/%.c
	$(cc) $(cflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.fpic.o: src/%.cpp
	$(cpp) $(cflags) $(fpicflags) $(cppflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.fpic.o: src/%.c
	$(cc) $(cflags) $(fpicflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: test/%.cpp
	$(cpp) $(cflags) $(cppflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: test/%.c
	$(cc) $(cflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(libd)/%.a:
	ar rc $@ $($(*)_objs)

ifeq (Darwin,$(lsb_dist))
$(libd)/%.dylib:
	$(cpplink) -dynamiclib $(cflags) $(lflags) -o $@.$($(*)_dylib).dylib -current_version $($(*)_dylib) -compatibility_version $($(*)_ver) $($(*)_dbjs) $($(*)_dlnk) $(sock_lib) $(math_lib) $(thread_lib) $(malloc_lib) $(dynlink_lib) && \
	cd $(libd) && ln -f -s $(@F).$($(*)_dylib).dylib $(@F).$($(*)_ver).dylib && ln -f -s $(@F).$($(*)_ver).dylib $(@F)
else
$(libd)/%.$(dll):
	$(cpplink) $(soflag) $(rpath) $(cflags) $(lflags) -o $@.$($(*)_spec) -Wl,-soname=$(@F).$($(*)_ver) $($(*)_dbjs) $($(*)_dlnk) $(sock_lib) $(math_lib) $(thread_lib) $(malloc_lib) $(dynlink_lib) && \
	cd $(libd) && ln -f -s $(@F).$($(*)_spec) $(@F).$($(*)_ver) && ln -f -s $(@F).$($(*)_ver) $(@F)
endif

$(bind)/%$(exe):
	$(cpplink) $(cflags) $(lflags) $(rpath) -o $@ $($(*)_objs) -L$(libd) $($(*)_lnk) $(cpp_lnk) $(sock_lib) $(math_lib) $(thread_lib) $(malloc_lib) $(dynlink_lib)

$(dependd)/%.d: src/%.cpp
	$(cpp) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: src/%.c
	$(cc) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.fpic.d: src/%.cpp
	$(cpp) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).fpic.o -MF $@

$(dependd)/%.fpic.d: src/%.c
	$(cc) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).fpic.o -MF $@

$(dependd)/%.d: test/%.cpp
	$(cpp) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: test/%.c
	$(cc) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@


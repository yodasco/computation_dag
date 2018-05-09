S3_BUCKET:=YOUR_BUCKET
ANIMAL_PATH_LOCAL:=_data/animals/animals.csv
ANIMAL_PATH_REMOTE:=s3://$(S3_BUCKET)/animals/animals.csv
NOISES_PATH_LOCAL:=_data/noises/noises.csv
NOISES_PATH_REMOTE:=s3://$(S3_BUCKET)/noises.csv
OUT_PATH_LOCAL:=_out/
OUT_PATH_REMOTE:=s3://$(S3_BUCKET)/match/
MY_EMAIL:=eliorm76@gmail.com

ifneq (,$(filter $(FLAVOR),local print))
	LIMIT_RECORDS:=5000
	ANIMAL_PATH:=$(ANIMAL_PATH_LOCAL)
	NOISES_PATH:=$(NOISES_PATH_LOCAL)
	OUT_PATH:=$(OUT_PATH_LOCAL)
else ifeq ($(FLAVOR),remote)
	LIMIT_RECORDS:=inf
	ANIMAL_PATH:=$(ANIMAL_PATH_REMOTE)
	NOISES_PATH:=$(NOISES_PATH_REMOTE)
	OUT_PATH:=$(OUT_PATH_REMOTE)
endif

# Utilities.
#
# Invoke flow. Invokes a given flow on spark, in the given mode.
# Arguments:
# 	flavor: local/remote.
#	executable: Flow to be executed.
#	Arguments: arguments to be passed to the flow.
#
define invoke_flow
	set noglob
	@case $(1) in \
		local) \
		PYSPARK_DRIVER_PYTHON=python spark-submit \
		--master local[*] \
		--driver-memory 8g \
		$2 \
		$3 \
		;; \
		print) \
		PYSPARK_DRIVER_PYTHON=python spark-submit \
		--master local[*] \
		--driver-memory 8g \
		$2 \
		$3 --dot_output=$(basename $2).dot \
		;; \
		remote) \
		python ../pyspark-emr/emr_run_spark.py --aws_region=us-west-2 \
		--create_cluster \
	    --create_cluster_slave_type=r3.4xlarge \
	    --create_cluster_master_type=m3.xlarge \
	    --create_cluster_num_hosts=3 \
	    --create_cluster_ec2_key_name=us-west-2-yodas-com \
	    --create_cluster_ec2_subnet_id=subnet-de620da9 \
	    --bootstrap_script=full_flow_emr_bootstrap.sh \
	    --bid_price=1.0 \
	    --emr_release_label="emr-5.6.0" \
	    --spark_packages=mysql:mysql-connector-java:5.1.39 \
	    --send_success_email_to=$(MY_EMAIL) \
	    --python_path . \
	    --s3_work_bucket $4  \
	    --spark_main $2 \
	    --spark_main_args "$3" \
		;; \
		*) \
		echo invalid argument $1 for FLAVOR valid values are: remote/local; \
		;; \
	esac
endef

all: example.py guard-FLAVOR
	$(eval ARGS:=--input_animals=$(ANIMAL_PATH) \
	 			 --input_noises=$(NOISES_PATH) \
				 --output_match=$(OUT_PATH))
	$(call invoke_flow,$(FLAVOR),$<,$(ARGS),$(S3_BUCKET))

guard-%:
	@ if [ "${${*}}" = "" ]; then \
		echo "Variable $* not set"; \
		exit 1; \
	fi

example.dot:
	FLAVOR=print make all

example.pdf: example.dot
	dot $< -Tpdf -o$@

toplogy_graph: example.pdf
	open $<

clean:
	@rm -rf *.dot *pdf

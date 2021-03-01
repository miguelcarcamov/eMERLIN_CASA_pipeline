import os
import sys
from __casac__.table import table as tb

antennas_dict = {"Mk2":0, "Kn":1, "De":2, "Pi":3, "Da":4, "Cm":5}
ns_baselines = ["Mk2&Kn", "Mk2&De", "Mk2&Da", "Mk2&Cm", "Kn&De", "Kn&Cm", "Pi&Da", "Pi&Cm", "Da&Cm"]
# function to return key for any value
def get_key(dict, val):
	for key, value in dict.items():
		if val == value:
			return key

def list_to_antennas(baselines=None):
	antenna1 = []
	antenna2 = []
	if(baselines is None):
		casalog.post("ERROR: The list is empty", "SEVERE")
		sys.exit(-1)
	else:
		for b in baselines:
			baseline = b.split("&")
			antenna1.append(antennas_dict[baseline[0]])
			antenna2.append(antennas_dict[baseline[1]])

	return antenna1, antenna2

def check_present_ns_baselines(ms=""):
	tb_obj = tb()
	if tb_obj.open(ms) is False:
		casalog.post("ERROR: The Measurement Set could not been opened", "SEVERE")
		sys.exit(-1)

	query_flag = "select FLAG_ROW from "+ms+"/ANTENNA"
	query = "select ANTENNA1,ANTENNA2 from "+ms+" where ANTENNA1!=ANTENNA2 groupby ANTENNA1,ANTENNA2"
	antenna_table = tb_obj.taql(query)
	antenna_flag_table = tb_obj.taql(query_flag)

	antenna1_list = list(antenna_table.getcol("ANTENNA1"))
	antenna2_list = list(antenna_table.getcol("ANTENNA2"))
	antenna_flag = list(antenna_flag_table.getcol("FLAG_ROW"))

	antenna_table.close()
	present_ns_antenna1 = []
	present_ns_antenna2 = []
	ns_baselines_string_list = []

	for i in range(0, len(antenna1_list)):
		baseline_string = get_key(antennas_dict, antenna1_list[i])+"&"+get_key(antennas_dict, antenna2_list[i])
		if baseline_string in ns_baselines and antenna_flag[antenna1_list[i]] == 0 and antenna_flag[antenna2_list[i]] == 0:
			present_ns_antenna1.append(str(antenna1_list[i]))
			present_ns_antenna2.append(str(antenna2_list[i]))
			ns_baselines_string_list.append(baseline_string)

	antenna1_string= ",".join(present_ns_antenna1)
	antenna2_string= ",".join(present_ns_antenna2)
	baseline_string = ",".join(ns_baselines_string_list)

	tb_obj.close()

	return antenna1_string, antenna2_string, baseline_string

def swap_crosshands_correlations(input_ms="", output_ms="", datacolumn="", swap_flags_weights=True):
	tb_obj = tb()
	casalog.origin('swap_crosshands_correlations')
	os.system('rm -rf '+output_ms)
	os.system('cp -r '+input_ms+' '+output_ms)

	#antenna1_list = []
	#antenna2_list = []

	#list_baselines = baselines.replace(" ","").split(",")

	#for i in list_baselines:
	#	baseline = i.split("&")
	#	antenna1_list.append(str(antennas_dict[baseline[0]]))
	#	antenna2_list.append(str(antennas_dict[baseline[1]]))

	#antenna1 = ",".join(antenna1_list)
	#antenna2 = ",".join(antenna2_list)

	antenna1, antenna2, baselines = check_present_ns_baselines(ms=input_ms)
	casalog.post("Swapping antennas ANTENNA1: "+antenna1+" ANTENNA2: "+antenna2, "INFO")
	casalog.post("Baselines: "+baselines, "INFO")

	if tb_obj.open(output_ms, nomodify=False) is False:
		casalog.post("ERROR: The Measurement Set could not been opened", "SEVERE")
		sys.exit(-1)

	# Swap flags and weights
	if(swap_flags_weights):
		select_subquery = "select FLAG,WEIGHT from "+output_ms+" where any(ANTENNA1==["+antenna1+"] && ANTENNA2==["+antenna2+"])"
		select_subquery_temp = "select FLAG as origflag, WEIGHT as origweight from "+input_ms+" where any(ANTENNA1==["+antenna1+"] && ANTENNA2==["+antenna2+"]) giving as memory"
		tb_obj.taql("update ["+select_subquery+"],["+select_subquery_temp+"] temp set FLAG[2,]=temp.origflag[3,], FLAG[3,]=temp.origflag[2,], WEIGHT[2]=temp.origweight[3], WEIGHT[3]=temp.origweight[2]")
		ms.writehistory("Flags and weights in baselines {0} have been swapped with TaQL".format(baselines), msname=output_ms)

	# Swap DATA columns
	select_subquery = "select "+datacolumn+" from "+output_ms+" where any(ANTENNA1==["+antenna1+"] && ANTENNA2==["+antenna2+"])"
	select_subquery_temp = "select "+datacolumn+" as origdata from "+input_ms+" where any(ANTENNA1==["+antenna1+"] && ANTENNA2==["+antenna2+"]) giving as memory"
	tb_obj.taql("update ["+select_subquery+"],["+select_subquery_temp+"] temp set "+datacolumn+"[2,]=temp.origdata[3,], "+datacolumn+"[3,]=temp.origdata[2,]")

	tb_obj.close()

	ms.writehistory("Crosshands correlations in {0} column in baselines {1} have been swapped with TaQL".format(datacolumn, baselines), msname=output_ms)

def sanity_check(input_ms="", output_ms="", datacolumn=""):
	casalog.origin('sanity_check')
	tb_obj_input = tb()
	tb_obj_output = tb()
	field_table_query = "select NAME, ROWID() as ID from "+input_ms+"/FIELD WHERE !FLAG_ROW"
	spw_table_query = "select ROWID() as ID from "+input_ms+"/SPECTRAL_WINDOW WHERE !FLAG_ROW"
	o_input = tb_obj_input.open(input_ms)
	o_output = tb_obj_output.open(output_ms)

	if  o_input==False and o_output==False:
		casalog.post("ERROR: One of the two Measurement Sets could not been opened", "SEVERE")
		sys.exit(-1)

	field_table = tb_obj_input.taql(field_table_query)
	fields = field_table.getvarcol("ID")
	fields_names = field_table.getvarcol("NAME")
	spw_table = tb_obj_input.taql(spw_table_query)
	spws = spw_table.getvarcol("ID")
	fields_id = []
	spws_id = []
	f_names = []

	for f in fields_names.values():
		f_names.append(f[0])

	for val in fields.values():
		fields_id.append(val[0])

	for i in spws.values():
		spws_id.append(i[0])

	print(fields_id)
	print(f_names)
	print("Fields: ", len(fields_id))
	print("Spectral windows: ", len(spws_id))
	field_table.close()
	spw_table.close()

	#antenna1_list = []
	#antenna2_list = []

	#list_baselines = baselines.replace(" ","").split(",")

	#for i in list_baselines:
	#	baseline = i.split("&")
	#	antenna1_list.append(str(antennas_dict[baseline[0]]))
	#	antenna2_list.append(str(antennas_dict[baseline[1]]))

	antenna1_string, antenna2_string, baselines_string = check_present_ns_baselines(ms=input_ms)
	antenna1_list = antenna1_string.split(",")
	antenna2_list = antenna2_string.split(",")
	list_baselines = baselines_string.split(",")
	ok_list = []


	for f in range(0, len(fields_id)):
		casalog.post("=========== Checking field " + f_names[f]+"============", "INFO")
		for b in range(0, len(list_baselines)):
			casalog.post("===========Checking baseline " + list_baselines[b]+"============", "INFO")
			for s in range(0, len(spws_id)):
				casalog.post("===========Checking spectral window "+ str(spws_id[s])+"============", "INFO")
				#tb.open(input_ms)
				old_cols = tb_obj_input.taql("select WEIGHT,DATA,FLAG from "+input_ms+" WHERE ANTENNA1="+antenna1_list[b]+" AND ANTENNA2="+antenna2_list[b]+" AND !FLAG_ROW AND FIELD_ID="+str(fields_id[f])+" AND DATA_DESC_ID="+str(spws_id[s]))
				#tb.open(output_ms)
				new_cols = tb_obj_output.taql("select WEIGHT,DATA,FLAG from "+output_ms+" WHERE ANTENNA1="+antenna1_list[b]+" AND ANTENNA2="+antenna2_list[b]+" AND !FLAG_ROW AND FIELD_ID="+str(fields_id[f])+" AND DATA_DESC_ID="+str(spws_id[s]))
				old_nrows = old_cols.nrows()
				new_nrows = new_cols.nrows()
				if old_nrows != 0 and new_nrows != 0:
					old_data = old_cols.getcol("DATA")
					new_data = new_cols.getcol("DATA")
					# Print the shapes
					#print("Old data shape:", old_data.shape)
					#print("New data shape:", new_data.shape)
					old_weights = old_cols.getcol("WEIGHT")
					new_weights = new_cols.getcol("WEIGHT")
					#print("Old weights shape:", old_data.shape)
					#print("New weights shape:", new_data.shape)
					old_flags = old_cols.getcol("FLAG")
					new_flags = new_cols.getcol("FLAG")

					old_data_nonflagged = np.where(old_flags==False, old_data, 0+0j)
					new_data_nonflagged = np.where(new_flags==False, new_data, 0+0j)
					#Checking number of nonzero elements in swapped correlations
					# Old RL with new LR
					print("Old data nonflagged shape:", old_data_nonflagged.shape)
					print("New data nonflagged shape:", new_data_nonflagged.shape)
					nonzeros_old_rl = np.count_nonzero(old_data_nonflagged[1])
					nonzeros_new_lr = np.count_nonzero(new_data_nonflagged[2])
					nonzeros_dif_first = nonzeros_old_rl - nonzeros_new_lr
					# Old LR with new RL
					nonzeros_old_lr = np.count_nonzero(old_data_nonflagged[2])
					nonzeros_new_rl = np.count_nonzero(new_data_nonflagged[1])
					nonzeros_dif_second = nonzeros_old_lr - nonzeros_new_rl
					equality_rl_lr = old_data_nonflagged[1] == new_data_nonflagged[2]
					equality_lr_rl = old_data_nonflagged[2] == new_data_nonflagged[1]
					#print(new_data_nonflagged)
					#print(new_data_nonflagged)
					#print("old RL vs new LR:", nonzeros_old_rl, nonzeros_new_lr)
					#print("old LR vs new RL:", nonzeros_old_lr, nonzeros_new_rl)
					if(equality_rl_lr.all() and equality_lr_rl.all() and nonzeros_dif_first==0 and nonzeros_dif_second==0):
						casalog.post("Baseline "+list_baselines[b]+" OK", "INFO")
						ok_list.append(True)
					else:
						casalog.post("Something is WRONG in this baseline", "SEVERE")
						#print(equality_rl_lr.all())
						#print(equality_lr_rl.all())
						#print("First dif:", nonzeros_dif_first)
						#print("Second dif:", nonzeros_dif_second)
						ok_list.append(False)
				else:
					casalog.post("No DATA for this query", "INFO")

	tb_obj_input.close()
	tb_obj_output.close()
	ok_array = np.array(ok_list)
	return ok_array.all()

input_ms = sys.argv[3]
output_ms = sys.argv[4]

#antenna1 = "0,0,0,1,1,3"
#antenna2 = "1,2,5,2,5,5"
#baselines = "Mk2&Kn, Mk2&De, Mk2&Cm, Kn&De, Kn&Cm, Pi&Cm"

swap_crosshands_correlations(input_ms=input_ms, output_ms=output_ms, datacolumn="DATA")

casalog.post("Doing sanity check", "INFO")
check = sanity_check(input_ms=input_ms, output_ms=output_ms, datacolumn="DATA")

if check:
	casalog.post("Swap has been done successfully", "INFO")
else:
	casalog.post("There has been an error when swapping columns", "SEVERE")

# MetaPhlAn2 Version Notes

## About MetaPhlAn2

The marker name in default database of MetaPhlAn2 contains special character that can't be recognized by htsjdk.

注意, MetaPhlAn2 已经更新到2019版, 代码变动较大, 多了一些参数, 因此后续测试的时候需要考虑这些新参数的配置. 

~~当前测试中的 `01.profiling/HMP_HMASM_MetaWUGSCStool/SOAPMetas_MEPHProcessM2` 目录下的脚本, bowtie2 的比对参数已经调整与 MetaPhlAn2 一致. ~~

~~与此同时, 为了保证新参数与 SOAPMetas 一致， `01.profiling/HMP_HMASM_MetaWUGSCStool/metaphlan2` 目录下的脚本 `--read_min_len` 参数设置为 0 .~~

metaphlan2018 和 metaphlan2019 的区别包括: 

+ 2019 的脚本中删除了 markers_to_exclude 基因列表, 这些基因是在 2018.12.11 月的 commit d24f233 中提交的
+ 2019 的数据集更新了 marker 的名称
+ 2019 的数据集取消了 Viruses 的 marker
+ 2019 的数据集 fna 序列文件中增加了 marker 的 UniRef 信息以及 taxonomy 信息和 genome GCA 编号
+ 2019 的数据集 pkl 文件的 marker 部分取消了 t__ 级别的分类单位, 即所有 marker 的 clade 信息最低为 s__ 级别
+ 2019 的数据集 pkl 文件的 taxonomy 部分添加了 taxid 信息, 读入文件后与原 genolen 形成二元组
+ 2019 的脚本中增加了对 taxid 处理的支持, 增加了对 CAMI 标准格式的支持, 增加了处理比对 reads 数量的直接统计
+ 2019 的脚本中增加了对二次比对 (secondary alignment) 的过滤和对比对质量分数 MAPQ 的过滤条件
+ 2019 的脚本中增加了部分控制参数, 修改了部分非核心代码的逻辑


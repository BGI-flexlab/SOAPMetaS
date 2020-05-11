package org.bgi.flexlab.metas.profiling.profilingmethod;

import com.google.gson.stream.JsonReader;
import htsjdk.samtools.SAMRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDPartitioner;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import org.bgi.flexlab.metas.profiling.filter.MetasSAMRecordIdentityFilter;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelBase;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName: MEPHWrongProfilingMethod
 * Description:
 *
 * @author heshixu@genomics.cn
 */

@Deprecated
public class MEPHWrongProfilingMethod extends ProfilingMethodBase implements Serializable {

    public static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(MEPHWrongProfilingMethod.class);

    private boolean doGCRecalibration;
    private GCBiasModelBase gcBiasRecaliModel;

    //private Broadcast<HashMap<String, ArrayList<String>>> markers2extsBroad = null;
    //private Broadcast<HashMap<String, Integer>> markers2lenBroad = null;
    //private Broadcast<HashMap<String, String>> markers2cladeBroad = null;
    private Broadcast<HashMap<String, Tuple3<String, Integer, ArrayList<String>>>> markersInformationBroad = null; // marker: cladename, len, extsList

    private Broadcast<ArrayList<Tuple2<ArrayList<String>, Integer>>> taxonomyInformationBroad = null;
    private Broadcast<HashMap<String, String>> cladeName2HighRankBroad = null; // "s__Species_name: k__xxx|p__xxx|c__xxx|o_xxx|f__xxx|g__xxx|"

    private Broadcast<HashMap<String, Integer>> sampleNamesBroadcast = null;

    private HashSet<String> excludeMarkers;

    //private String metaphlanMpaDBFile;
    private String mpaMarkersListFile; // MetaPhlAn2 markers list file extracted from mpa_v20_m200.pkl "marker"
    private String mpaTaxonomyListFile; // MetaPhlAn2 taxonomy list file extracted from mpa_v20_m200.pkl "taxonomy"

    private boolean doIdentityFiltering = false;
    private boolean doAlignLenFiltering = false;

    private MetasSAMRecordIdentityFilter identityFilter;
    //private MetasSAMRecordAlignLenFilter alignLenFilter;
    private int minAlignLen = 0;
    private Pattern cigarPattern;

    private int minMapQuality = -100;

    private MetasOptions options;

    public MEPHWrongProfilingMethod(MetasOptions options, JavaSparkContext jsc){
        super(options, jsc);

        this.options = options;

        this.mpaMarkersListFile = options.getMpaMarkersListFile();
        this.mpaTaxonomyListFile = options.getMpaTaxonomyListFile();

        this.doIdentityFiltering = options.isDoIdentityFiltering();
        this.doAlignLenFiltering = options.isDoAlignLenFiltering();

        if (doIdentityFiltering) {
            this.identityFilter = new MetasSAMRecordIdentityFilter(options.getMinIdentity());
        }
        if (doAlignLenFiltering) {
            //this.alignLenFilter = new MetasSAMRecordAlignLenFilter(options.getMinAlignLength());
            this.cigarPattern = Pattern.compile("(\\d+)(\\D+)");
            this.minAlignLen = options.getMinAlignLength();
        }

        this.excludeMarkers = new HashSet<>(390);
        Collections.addAll(this.excludeMarkers, "NC_001782.1",
                "GeneID:10498696", "GeneID:10498710", "GeneID:10498726", "GeneID:10498735",
                "GeneID:10498757", "GeneID:10498760", "GeneID:10498761", "GeneID:10498763",
                "GeneID:11294465", "GeneID:14181982", "GeneID:14182132", "GeneID:14182146",
                "GeneID:14182148", "GeneID:14182328", "GeneID:14182639", "GeneID:14182647",
                "GeneID:14182650", "GeneID:14182663", "GeneID:14182683", "GeneID:14182684",
                "GeneID:14182691", "GeneID:14182803", "GeneID:14296322", "GeneID:1489077",
                "GeneID:1489080", "GeneID:1489081", "GeneID:1489084", "GeneID:1489085",
                "GeneID:1489088", "GeneID:1489089", "GeneID:1489090", "GeneID:1489528",
                "GeneID:1489530", "GeneID:1489531", "GeneID:1489735", "GeneID:1491873",
                "GeneID:1491889", "GeneID:1491962", "GeneID:1491963", "GeneID:1491964",
                "GeneID:1491965", "GeneID:17099689", "GeneID:1724732", "GeneID:17494231",
                "GeneID:2546403", "GeneID:2703374", "GeneID:2703375", "GeneID:2703498",
                "GeneID:2703531", "GeneID:2772983", "GeneID:2772989", "GeneID:2772991",
                "GeneID:2772993", "GeneID:2772995", "GeneID:2773037", "GeneID:2777387",
                "GeneID:2777399", "GeneID:2777400", "GeneID:2777439", "GeneID:2777493",
                "GeneID:2777494", "GeneID:3077424", "GeneID:3160801", "GeneID:3197323",
                "GeneID:3197355", "GeneID:3197400", "GeneID:3197428", "GeneID:3783722",
                "GeneID:3783750", "GeneID:3953004", "GeneID:3959334", "GeneID:3964368",
                "GeneID:3964370", "GeneID:4961452", "GeneID:5075645", "GeneID:5075646",
                "GeneID:5075647", "GeneID:5075648", "GeneID:5075649", "GeneID:5075650",
                "GeneID:5075651", "GeneID:5075652", "GeneID:5075653", "GeneID:5075654",
                "GeneID:5075655", "GeneID:5075656", "GeneID:5075657", "GeneID:5075658",
                "GeneID:5075659", "GeneID:5075660", "GeneID:5075661", "GeneID:5075662",
                "GeneID:5075663", "GeneID:5075664", "GeneID:5075665", "GeneID:5075667",
                "GeneID:5075668", "GeneID:5075669", "GeneID:5075670", "GeneID:5075671",
                "GeneID:5075672", "GeneID:5075673", "GeneID:5075674", "GeneID:5075675",
                "GeneID:5075676", "GeneID:5075677", "GeneID:5075678", "GeneID:5075679",
                "GeneID:5075680", "GeneID:5075681", "GeneID:5075682", "GeneID:5075683",
                "GeneID:5075684", "GeneID:5075685", "GeneID:5075686", "GeneID:5075687",
                "GeneID:5075688", "GeneID:5075689", "GeneID:5075690", "GeneID:5075691",
                "GeneID:5075692", "GeneID:5075693", "GeneID:5075694", "GeneID:5075695",
                "GeneID:5075696", "GeneID:5075697", "GeneID:5075698", "GeneID:5075700",
                "GeneID:5075701", "GeneID:5075702", "GeneID:5075703", "GeneID:5075704",
                "GeneID:5075705", "GeneID:5075707", "GeneID:5075708", "GeneID:5075709",
                "GeneID:5075710", "GeneID:5075711", "GeneID:5075712", "GeneID:5075713",
                "GeneID:5075714", "GeneID:5075715", "GeneID:5075716", "GeneID:5176189",
                "GeneID:6803896", "GeneID:6803915", "GeneID:7944151", "GeneID:927334",
                "GeneID:927335", "GeneID:927337", "GeneID:940263", "GeneID:9538324",
                "NC_003977.1", "gi|103485498|ref|NC_008048.1|:1941166-1942314",
                "gi|108802856|ref|NC_008148.1|:1230231-1230875",
                "gi|124806686|ref|XM_001350760.1|",
                "gi|126661648|ref|NZ_AAXW01000149.1|:c1513-1341",
                "gi|149172845|ref|NZ_ABBW01000029.1|:970-1270",
                "gi|153883242|ref|NZ_ABDQ01000074.1|:79-541",
                "gi|167031021|ref|NC_010322.1|:1834668-1835168",
                "gi|171344510|ref|NZ_ABJO01001391.1|:1-116",
                "gi|171346813|ref|NZ_ABJO01001728.1|:c109-1",
                "gi|190640924|ref|NZ_ABRC01000948.1|:c226-44",
                "gi|223045343|ref|NZ_ACEN01000042.1|:1-336",
                "gi|224580998|ref|NZ_GG657387.1|:c114607-114002",
                "gi|224993759|ref|NZ_ACFY01000068.1|:c357-1",
                "gi|237784637|ref|NC_012704.1|:141000-142970",
                "gi|237784637|ref|NC_012704.1|:c2048315-2047083",
                "gi|240136783|ref|NC_012808.1|:1928224-1928961",
                "gi|255319020|ref|NZ_ACVR01000025.1|:28698-29132",
                "gi|260590341|ref|NZ_ACEO02000062.1|:c387-151",
                "gi|262368201|ref|NZ_GG704964.1|:733100-733978",
                "gi|262369811|ref|NZ_GG704966.1|:c264858-264520",
                "gi|288559258|ref|NC_013790.1|:448046-451354",
                "gi|288559258|ref|NC_013790.1|:532047-533942",
                "gi|294794157|ref|NZ_GG770200.1|:245344-245619",
                "gi|304372805|ref|NC_014448.1|:444677-445120",
                "gi|304372805|ref|NC_014448.1|:707516-708268",
                "gi|304372805|ref|NC_014448.1|:790263-792257",
                "gi|304372805|ref|NC_014448.1|:c367313-364470",
                "gi|304372805|ref|NC_014448.1|:c659144-658272",
                "gi|304372805|ref|NC_014448.1|:c772578-770410",
                "gi|304372805|ref|NC_014448.1|:c777901-777470",
                "gi|306477407|ref|NZ_GG770409.1|:c1643877-1643338",
                "gi|317120849|ref|NC_014831.1|:c891121-890144",
                "gi|323356441|ref|NZ_GL698442.1|:560-682",
                "gi|324996766|ref|NZ_BABV01000451.1|:10656-11579",
                "gi|326579405|ref|NZ_AEGQ01000006.1|:2997-3791",
                "gi|326579407|ref|NZ_AEGQ01000008.1|:c45210-44497",
                "gi|326579433|ref|NZ_AEGQ01000034.1|:346-3699",
                "gi|329889017|ref|NZ_GL883086.1|:586124-586804",
                "gi|330822653|ref|NC_015422.1|:2024431-2025018",
                "gi|335053104|ref|NZ_AFIL01000010.1|:c33862-32210",
                "gi|339304121|ref|NZ_AEOR01000258.1|:c294-1",
                "gi|339304277|ref|NZ_AEOR01000414.1|:1-812",
                "gi|342211239|ref|NZ_AFUK01000001.1|:790086-790835",
                "gi|342211239|ref|NZ_AFUK01000001.1|:c1579497-1578787",
                "gi|342213707|ref|NZ_AFUJ01000005.1|:48315-48908",
                "gi|355707189|ref|NZ_JH376566.1|:326756-326986",
                "gi|355707384|ref|NZ_JH376567.1|:90374-91453",
                "gi|355707384|ref|NZ_JH376567.1|:c388018-387605",
                "gi|355708440|ref|NZ_JH376569.1|:c80380-79448",
                "gi|358051729|ref|NZ_AEUN01000100.1|:c120-1",
                "gi|365983217|ref|XM_003668394.1|",
                "gi|377571722|ref|NZ_BAFD01000110.1|:c1267-29",
                "gi|377684864|ref|NZ_CM001194.1|:c1159954-1159619",
                "gi|377684864|ref|NZ_CM001194.1|:c4966-4196",
                "gi|378759497|ref|NZ_AFXE01000152.1|:1628-2215",
                "gi|378835506|ref|NC_016829.1|:112560-113342",
                "gi|378835506|ref|NC_016829.1|:114945-115193",
                "gi|378835506|ref|NC_016829.1|:126414-127151",
                "gi|378835506|ref|NC_016829.1|:272056-272403",
                "gi|378835506|ref|NC_016829.1|:272493-272786",
                "gi|378835506|ref|NC_016829.1|:358647-360863",
                "gi|378835506|ref|NC_016829.1|:37637-38185",
                "gi|378835506|ref|NC_016829.1|:60012-60497",
                "gi|378835506|ref|NC_016829.1|:606819-607427",
                "gi|378835506|ref|NC_016829.1|:607458-607760",
                "gi|378835506|ref|NC_016829.1|:826192-826821",
                "gi|378835506|ref|NC_016829.1|:c451932-451336",
                "gi|378835506|ref|NC_016829.1|:c460520-459951",
                "gi|378835506|ref|NC_016829.1|:c483843-482842",
                "gi|378835506|ref|NC_016829.1|:c544660-543638",
                "gi|378835506|ref|NC_016829.1|:c556383-555496",
                "gi|378835506|ref|NC_016829.1|:c632166-631228",
                "gi|378835506|ref|NC_016829.1|:c805066-802691",
                "gi|384124469|ref|NC_017160.1|:c2157447-2156863",
                "gi|385263288|ref|NZ_AJST01000001.1|:594143-594940",
                "gi|385858114|ref|NC_017519.1|:10252-10746",
                "gi|385858114|ref|NC_017519.1|:104630-104902",
                "gi|385858114|ref|NC_017519.1|:154292-156016",
                "gi|385858114|ref|NC_017519.1|:205158-206462",
                "gi|385858114|ref|NC_017519.1|:507239-507703",
                "gi|385858114|ref|NC_017519.1|:518924-519772",
                "gi|385858114|ref|NC_017519.1|:524712-525545",
                "gi|385858114|ref|NC_017519.1|:528387-528785",
                "gi|385858114|ref|NC_017519.1|:532275-533429",
                "gi|385858114|ref|NC_017519.1|:586402-586824",
                "gi|385858114|ref|NC_017519.1|:621696-622226",
                "gi|385858114|ref|NC_017519.1|:673673-676105",
                "gi|385858114|ref|NC_017519.1|:706602-708218",
                "gi|385858114|ref|NC_017519.1|:710627-711997",
                "gi|385858114|ref|NC_017519.1|:744974-745456",
                "gi|385858114|ref|NC_017519.1|:791055-791801",
                "gi|385858114|ref|NC_017519.1|:805643-807430",
                "gi|385858114|ref|NC_017519.1|:c172050-170809",
                "gi|385858114|ref|NC_017519.1|:c334545-333268",
                "gi|385858114|ref|NC_017519.1|:c383474-383202",
                "gi|385858114|ref|NC_017519.1|:c450880-450389",
                "gi|385858114|ref|NC_017519.1|:c451975-451001",
                "gi|385858114|ref|NC_017519.1|:c470488-470036",
                "gi|385858114|ref|NC_017519.1|:c485596-484598",
                "gi|385858114|ref|NC_017519.1|:c58658-58065",
                "gi|385858114|ref|NC_017519.1|:c592754-591081",
                "gi|385858114|ref|NC_017519.1|:c59590-58820",
                "gi|385858114|ref|NC_017519.1|:c601339-600575",
                "gi|385858114|ref|NC_017519.1|:c76080-75160",
                "gi|385858114|ref|NC_017519.1|:c97777-96302",
                "gi|391227518|ref|NZ_CM001514.1|:c1442504-1440237",
                "gi|391227518|ref|NZ_CM001514.1|:c3053472-3053023",
                "gi|394749766|ref|NZ_AHHC01000069.1|:3978-6176",
                "gi|398899615|ref|NZ_AKJK01000021.1|:28532-29209",
                "gi|406580057|ref|NZ_AJRD01000017.1|:c17130-15766",
                "gi|406584668|ref|NZ_AJQZ01000017.1|:c1397-771",
                "gi|408543458|ref|NZ_AJLO01000024.1|:67702-68304",
                "gi|410936685|ref|NZ_AJRF02000012.1|:21785-22696",
                "gi|41406098|ref|NC_002944.2|:c4468304-4467864",
                "gi|416998679|ref|NZ_AEXI01000003.1|:c562937-562176",
                "gi|417017738|ref|NZ_AEYL01000489.1|:c111-1",
                "gi|417018375|ref|NZ_AEYL01000508.1|:100-238",
                "gi|418576506|ref|NZ_AHKB01000025.1|:c7989-7669",
                "gi|419819595|ref|NZ_AJRE01000517.1|:1-118",
                "gi|421806549|ref|NZ_AMTB01000006.1|:c181247-180489",
                "gi|422320815|ref|NZ_GL636045.1|:28704-29048",
                "gi|422320874|ref|NZ_GL636046.1|:4984-5742",
                "gi|422323244|ref|NZ_GL636061.1|:479975-480520",
                "gi|422443048|ref|NZ_GL383112.1|:663738-664823",
                "gi|422552858|ref|NZ_GL383469.1|:c216727-215501",
                "gi|422859491|ref|NZ_GL878548.1|:c271832-271695",
                "gi|423012810|ref|NZ_GL982453.1|:3888672-3888935",
                "gi|423012810|ref|NZ_GL982453.1|:4541873-4542328",
                "gi|423012810|ref|NZ_GL982453.1|:c2189976-2188582",
                "gi|423012810|ref|NZ_GL982453.1|:c5471232-5470300",
                "gi|423262555|ref|NC_019552.1|:24703-25212",
                "gi|423262555|ref|NC_019552.1|:28306-30696",
                "gi|423262555|ref|NC_019552.1|:284252-284581",
                "gi|423262555|ref|NC_019552.1|:311161-311373",
                "gi|423262555|ref|NC_019552.1|:32707-34497",
                "gi|423262555|ref|NC_019552.1|:34497-35237",
                "gi|423262555|ref|NC_019552.1|:53691-56813",
                "gi|423262555|ref|NC_019552.1|:c388986-386611",
                "gi|423262555|ref|NC_019552.1|:c523106-522528",
                "gi|423689090|ref|NZ_CM001513.1|:c1700632-1699448",
                "gi|423689090|ref|NZ_CM001513.1|:c1701670-1700651",
                "gi|423689090|ref|NZ_CM001513.1|:c5739118-5738390",
                "gi|427395956|ref|NZ_JH992914.1|:c592682-591900",
                "gi|427407324|ref|NZ_JH992904.1|:c2681223-2679463",
                "gi|451952303|ref|NZ_AJRB03000021.1|:1041-1574",
                "gi|452231579|ref|NZ_AEKA01000123.1|:c18076-16676",
                "gi|459791914|ref|NZ_CM001824.1|:c899379-899239",
                "gi|471265562|ref|NC_020815.1|:3155799-3156695",
                "gi|472279780|ref|NZ_ALPV02000001.1|:33911-36751",
                "gi|482733945|ref|NZ_AHGZ01000071.1|:10408-11154",
                "gi|483051300|ref|NZ_ALYK02000034.1|:c37582-36650",
                "gi|483051300|ref|NZ_ALYK02000034.1|:c38037-37582",
                "gi|483993347|ref|NZ_AMXG01000045.1|:251724-253082",
                "gi|484100856|ref|NZ_JH670250.1|:600643-602949",
                "gi|484115941|ref|NZ_AJXG01000093.1|:567-947",
                "gi|484228609|ref|NZ_JH730929.1|:c103784-99021",
                "gi|484228797|ref|NZ_JH730960.1|:c16193-12429",
                "gi|484228814|ref|NZ_JH730962.1|:c29706-29260",
                "gi|484228929|ref|NZ_JH730981.1|:18645-22060",
                "gi|484228939|ref|NZ_JH730983.1|:42943-43860",
                "gi|484266598|ref|NZ_AKGC01000024.1|:118869-119636",
                "gi|484327375|ref|NZ_AKVP01000093.1|:1-1281",
                "gi|484328234|ref|NZ_AKVP01000127.1|:c325-110",
                "gi|487376144|ref|NZ_KB911257.1|:600445-601482",
                "gi|487376194|ref|NZ_KB911260.1|:146228-146533",
                "gi|487381776|ref|NZ_KB911485.1|:101242-103083",
                "gi|487381776|ref|NZ_KB911485.1|:c32472-31627",
                "gi|487381800|ref|NZ_KB911486.1|:39414-39872",
                "gi|487381828|ref|NZ_KB911487.1|:15689-17026",
                "gi|487381846|ref|NZ_KB911488.1|:13678-13821",
                "gi|487382089|ref|NZ_KB911497.1|:23810-26641",
                "gi|487382176|ref|NZ_KB911501.1|:c497-381",
                "gi|487382213|ref|NZ_KB911502.1|:12706-13119",
                "gi|487382247|ref|NZ_KB911505.1|:c7595-6663",
                "gi|490551798|ref|NZ_AORG01000011.1|:40110-41390",
                "gi|491099398|ref|NZ_KB849654.1|:c720460-719912",
                "gi|491124812|ref|NZ_KB849705.1|:1946500-1946937",
                "gi|491155563|ref|NZ_KB849732.1|:46469-46843",
                "gi|491155563|ref|NZ_KB849732.1|:46840-47181",
                "gi|491155563|ref|NZ_KB849732.1|:47165-48616",
                "gi|491155563|ref|NZ_KB849732.1|:55055-56662",
                "gi|491155563|ref|NZ_KB849732.1|:56662-57351",
                "gi|491155563|ref|NZ_KB849732.1|:6101-7588",
                "gi|491155563|ref|NZ_KB849732.1|:7657-8073",
                "gi|491349766|ref|NZ_KB850082.1|:441-941",
                "gi|491395079|ref|NZ_KB850142.1|:1461751-1462554",
                "gi|512608407|ref|NZ_KE150401.1|:c156891-156016",
                "gi|518653462|ref|NZ_ATLM01000004.1|:c89669-89247",
                "gi|520818261|ref|NZ_ATLQ01000015.1|:480744-481463",
                "gi|520822538|ref|NZ_ATLQ01000063.1|:103173-103283",
                "gi|520826510|ref|NZ_ATLQ01000092.1|:c13892-13563",
                "gi|544644736|ref|NZ_KE747865.1|:68388-69722",
                "gi|545347918|ref|NZ_KE952096.1|:c83873-81831",
                "gi|550735774|gb|AXMM01000002.1|:c743886-743575",
                "gi|552875787|ref|NZ_KI515684.1|:c584270-583890",
                "gi|552876418|ref|NZ_KI515685.1|:36713-37258",
                "gi|552876418|ref|NZ_KI515685.1|:432422-433465",
                "gi|552876418|ref|NZ_KI515685.1|:c1014617-1014117",
                "gi|552876418|ref|NZ_KI515685.1|:c931935-931327",
                "gi|552876815|ref|NZ_KI515686.1|:613740-614315",
                "gi|552879811|ref|NZ_AXME01000001.1|:1146402-1146932",
                "gi|552879811|ref|NZ_AXME01000001.1|:40840-41742",
                "gi|552879811|ref|NZ_AXME01000001.1|:49241-49654",
                "gi|552891898|ref|NZ_AXMG01000001.1|:99114-99290",
                "gi|552891898|ref|NZ_AXMG01000001.1|:c1460921-1460529",
                "gi|552895565|ref|NZ_AXMI01000001.1|:619555-620031",
                "gi|552895565|ref|NZ_AXMI01000001.1|:c14352-13837",
                "gi|552896371|ref|NZ_AXMI01000002.1|:c148595-146280",
                "gi|552897201|ref|NZ_AXMI01000004.1|:c231437-230883",
                "gi|552902020|ref|NZ_AXMK01000001.1|:c1625038-1624022",
                "gi|556346902|ref|NZ_KI535485.1|:c828278-827901",
                "gi|556478613|ref|NZ_KI535633.1|:3529392-3530162",
                "gi|560534311|ref|NZ_AYSF01000111.1|:26758-29049",
                "gi|564165687|gb|AYLX01000355.1|:10906-11166",
                "gi|564169776|gb|AYLX01000156.1|:1-185",
                "gi|564938696|gb|AWYH01000018.1|:c75674-75039", "gi|67993724|ref|XM_664440.1|",
                "gi|68059117|ref|XM_666447.1|", "gi|68062389|ref|XM_668109.1|",
                "gi|71730848|gb|AAAM03000019.1|:c14289-12877", "gi|82753723|ref|XM_722699.1|",
                "gi|82775382|ref|NC_007606.1|:2249487-2250014", "gi|82793634|ref|XM_723027.1|",
                "GeneID:1489527");

        String excludeMarkerFile = options.getMpaExcludeMarkersFile();
        if (excludeMarkerFile != null) {
            try (BufferedReader br = new BufferedReader(new FileReader(excludeMarkerFile))) {
                String currentline;
                while ((currentline = br.readLine()) != null) {
                    excludeMarkers.add(currentline);
                }
            } catch (FileNotFoundException e) {
                LOG.error("[SOAPMetas::" + MEPHWrongProfilingMethod.class.getName() + "] Markers-to-exclude list file not found. " + e.toString());
            } catch (IOException e) {
                LOG.error("[SOAPMetas::" + MEPHWrongProfilingMethod.class.getName() + "] Markers-to-exclude file IO error. " + e.toString());
            }
        }
    }

    /**
     * TODO: 需要完善基于METAPHLAN策略的丰度计算方法。
     *
     * @param readMetasSamPairRDD
     * @return
     */
    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSAMPairRecord> readMetasSamPairRDD, Partitioner partitioner){
        return null;
    }

    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaRDD<SAMRecord> samRecordJavaRDD, JavaSparkContext ctx) {

        SampleIDPartitioner sampleIDPartitioner = new SampleIDPartitioner(this.sampleIDbySampleName.size() + 1);

        if (this.sampleNamesBroadcast == null) {
            this.sampleNamesBroadcast = ctx.broadcast(this.sampleIDbySampleName);
        }

        if (this.markersInformationBroad == null) {
            //TODO: 这一部分多 partition 之间的同步判定需要进一步考虑

            // MetaPhlAn2 2018 database:
            // markers2clades/exts/lens: 1035649
            // taxa2clades: 16904
            // allClades: 27085
            HashMap<String, Tuple3<String, Integer, ArrayList<String>>> markersInformation = new HashMap<>(1035700); // marker: cladename, len, extsList
            ArrayList<Tuple2<ArrayList<String>, Integer>> taxonomyInformation = new ArrayList<>(17000); // ([k__xxx, p__xxx, c__xxx, ..., s__xxx, t__xxx], genoLength)
            HashMap<String, String> cladeName2HighRank = new HashMap<>(130000); // "s__Species_name: k__xxx|p__xxx|c__xxx|o_xxx|f__xxx|g__xxx|"


            this.readMarkerFile(this.mpaMarkersListFile, markersInformation);
            this.readTaxonomyFile(this.mpaTaxonomyListFile, taxonomyInformation, cladeName2HighRank);

            //Broadcast<HashMap<String, Tuple3<String, Integer, ArrayList<String>>>> markersInformationBroad = ctx.broadcast(markersInformation);
            //Broadcast<ArrayList<Tuple2<ArrayList<String>, Integer>>> taxonomyInformationBroad = ctx.broadcast(taxonomyInformation);
            //Broadcast<HashMap<String, String>> cladeName2HighRankBroad = ctx.broadcast(cladeName2HighRank);
            this.markersInformationBroad = ctx.broadcast(markersInformation);
            this.taxonomyInformationBroad = ctx.broadcast(taxonomyInformation);
            this.cladeName2HighRankBroad = ctx.broadcast(cladeName2HighRank);
        }


        /*
        Input:
         JavaRDD<SAMRecord>

        After mapToPair: (read2marker)
         key: sampleID"\t"rgID"\t"cladeName
         value: HashMap<markerName, tuple<1, gc_recali_value>>
         Partition: default

        After reduceByKey: (marker2)
         key: sampleID"\t"rgID"\t"cladeName
         value: HashMap<markerName, tuple<count, gc_recali_count>>
         Partition: sampleID


        Output:
         Type: JavaPairRDD
         key: sampleID
         value: ProfilingResultRecord (clusterName includes taxonomy information)
        */
        return samRecordJavaRDD.mapToPair(samRecord -> countTupleGenerator(samRecord.getStringAttribute("RG"), samRecord))
                .filter(tuple -> tuple._1 != null)
                .reduceByKey(sampleIDPartitioner, (a, b) -> {
                    HashMap<String, Tuple2<Integer, Double>> c;
                    if (a.size() < b.size()) {
                        c = new HashMap<>(b);
                        a.forEach((k, v) -> c.merge(k, v, (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)));
                    } else {
                        c = new HashMap<>(a);
                        b.forEach((k, v) -> c.merge(k, v,  (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)));
                    }
                    return c;
                }).mapPartitionsToPair(new MEPHWrongAbundanceFunction(this.markersInformationBroad, this.taxonomyInformationBroad, this.cladeName2HighRankBroad, this.options), true);
    }

    private Tuple2<String, HashMap<String, Tuple2<Integer, Double>>> countTupleGenerator(String rgID, SAMRecord record) {

        String markerName = record.getReferenceName();
        if (this.excludeMarkers.contains(markerName)) {
            //LOG.info("[SOAPMetas::" + MEPHWrongProfilingMethod.class.getName() + "] Exclude special marker " + markerName);
            return new Tuple2<>(null, null);
        }

        // TODO: MetaPhlAn 在对多重比对以及可能的 PE 比对 reads 处理时, 由于使用了 set() 来保存 read name, 因此隐含了去除重复元素的步骤
        if (record.isSecondaryAlignment() || record.getMappingQuality() <= this.minMapQuality ) { //TODO: 添加质量分数的 option
            return new Tuple2<>(null, null);
        }

        if (doIdentityFiltering && this.identityFilter.filter(record)){
            return new Tuple2<>(null, null);
        }

        if (doAlignLenFiltering) { // && this.alignLenFilter.filter(record)) {
            int subAlignLen = 0;
            Matcher cigarMatcher = this.cigarPattern.matcher(record.getCigar().toString());
            while(cigarMatcher.find()){
                if (cigarMatcher.group(2).toUpperCase().equals("M")){
                    subAlignLen = Math.max(subAlignLen, Integer.parseInt(cigarMatcher.group(1)));
                }
            }
            if (subAlignLen < this.minAlignLen) {
                return new Tuple2<>(null, null);
            }
        }

        Double recaliReadCount = 1.0;

        if (this.doGCRecalibration) {
            //recaliReadCount = this.gcBiasRecaliModel.recalibrateForSingle(
            //        SequenceUtil.calculateGc(record.getReadBases()),
            //        //this.referenceInfoMatrix.value().getSpeciesGenoGC(this.referenceInfoMatrix.value().getGeneSpeciesName(geneName))
            //);
        }

        HashMap<String, Tuple2<Integer, Double>> readCount = new HashMap<>(2);
        readCount.put(markerName, new Tuple2<>(1, recaliReadCount));
        return new Tuple2<>(
                this.sampleIDbySampleName.get(rgID) + "\t" + rgID + "\t" + this.markersInformationBroad.getValue().get(markerName)._1(),
                readCount
        );
    }

    @Override
    public void setSampleIDbySampleName(HashMap<String, Integer> sampleIDbySampleName) {
        this.sampleIDbySampleName = sampleIDbySampleName;
    }

    private void readMarkerFile(String mpaMarkersListFile, HashMap<String, Tuple3<String, Integer, ArrayList<String>>> markersInformation) {
        // gi|483970126|ref|NZ_KB891629.1|:c6456-5752      {'ext': {'GCF_000373585', 'GCF_000355695', 'GCF_000226995'}, 'score': 3.0, 'clade': 's__Streptomyces_sp_KhCrAH_244', 'len': 705, 'taxon': 'k__Bacteria|p__Actinobacteria|c__Actinobacteria|o__Actinomycetales|f__Streptomycetaceae|g__Streptomyces|s__Streptomyces_sp_KhCrAH_244'}
        // target structure: {marker: (cladename, len, [extsList])}
        try (JsonReader jsonReader = new JsonReader(new FileReader(mpaMarkersListFile))) {

            String markerName;
            int markerLen = -1;
            String cladeName = null;
            String markerItem;
            ArrayList<String> extArray;

            jsonReader.beginObject();

            while (jsonReader.hasNext()) {

                markerName = jsonReader.nextName();
                //System.out.println("Gene: " + markerName);
                if (this.excludeMarkers.contains(markerName)) {
                    jsonReader.skipValue();
                    continue;
                }

                extArray = new ArrayList<>(5);

                jsonReader.beginObject();
                while (jsonReader.hasNext()) {
                    markerItem = jsonReader.nextName();
                    switch (markerItem) {
                        case "ext": {
                            //System.out.print(markerItem + ": ");
                            jsonReader.beginArray();
                            while (jsonReader.hasNext()) {
                                extArray.add("t__" + jsonReader.nextString());
                            }
                            jsonReader.endArray();
                            //System.out.println(extArray.toString());
                            break;
                        }
                        case "clade": {
                            //System.out.print(markerItem + ": ");
                            cladeName = jsonReader.nextString();
                            //System.out.println(cladeName);
                            break;
                        }
                        case "len": {
                            //System.out.print(markerItem + ": ");
                            markerLen = jsonReader.nextInt();
                            //System.out.println(Integer.toString(markerLen));
                            break;
                        }
                        case "score": {
                            jsonReader.nextDouble();
                            break;
                        }
                        case "taxon":
                        case "oname": {
                            jsonReader.nextString();
                            break;
                        }
                    }
                }
                jsonReader.endObject();
                if (markerLen < 0 || cladeName == null) {
                    LOG.error("[SOAPMetas::" + MEPHWrongProfilingMethod.class.getName() + "] Gene " + markerName + "doesn't has information.");
                    continue;
                }
                extArray.trimToSize();
                markersInformation.put(markerName, new Tuple3<>(cladeName, markerLen, extArray));
                //System.out.println("Name: " + geneName + "\next: " + extArray.toString() + "\nClade: " + cladeName + "\nLength: " + Integer.toString(geneLen));
            }

            jsonReader.endObject();
        } catch (FileNotFoundException e) {
            LOG.error("[SOAPMetas::" + MEPHWrongProfilingMethod.class.getName() + "] mpa markers information file not found. " + e.toString());
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + MEPHWrongProfilingMethod.class.getName() + "] mpa markers information reader error. " + e.toString());
        }
    }

    private void readTaxonomyFile(String mpaTaxonomyListFile, ArrayList<Tuple2<ArrayList<String>, Integer>> taxonomyInformation, HashMap<String, String> cladeName2HighRank) {
        // k__Bacteria|p__Proteobacteria|c__Gammaproteobacteria|o__Enterobacteriales|f__Enterobacteriaceae|g__Shigella|s__Shigella_flexneri|t__GCF_000213455       ("1|22|333|4444|55555", 4656186)
        // target txonomy info:
        // target high rank:

        try (JsonReader jsonReader = new JsonReader(new FileReader(mpaTaxonomyListFile))) {

            String[] taxonLev;
            String tempItem;
            String taxIDs;
            int genoLen = 0;
            int count;

            jsonReader.beginObject();

            while (jsonReader.hasNext()) {

                taxonLev = jsonReader.nextName().split("\\|");
                ArrayList<String> taxonList = new ArrayList<>(9);
                Collections.addAll(taxonList, taxonLev);

                jsonReader.beginObject();
                while (jsonReader.hasNext()) {
                    tempItem = jsonReader.nextName();
                    switch (tempItem) {
                        case "genoLen": {
                            genoLen = jsonReader.nextInt();
                            break;
                        }
                        case "taxid": {
                            taxIDs = jsonReader.nextString();
                            break;
                        }
                    }
                }
                jsonReader.endObject();
                taxonomyInformation.add(new Tuple2<>(new ArrayList<>(taxonList), genoLen));

                count = taxonLev.length;
                StringBuilder highRank = new StringBuilder();
                for (int i = 0; i < count; i++) {
                    cladeName2HighRank.put(taxonLev[i], highRank.toString());
                    highRank.append(taxonLev[i]).append('|');
                }

            }

            jsonReader.endObject();

            //while ((currentLine = taxonbr.readLine()) != null) {
            //    lineEle = currentLine.split("\\s+");
            //    taxonLev = lineEle[0].split("\\|");
            //    genoLen = Integer.parseInt(lineEle[1]);
            //    taxonomyInformation.add(new Tuple2<>(new ArrayList<>(Arrays.asList(taxonLev)), genoLen));
            //    count = taxonLev.length;
            //    StringBuilder highRank = new StringBuilder();
            //    for (int i = 0; i < count; i++) {
            //        cladeName2HighRank.put(taxonLev[i], highRank.toString());
            //        highRank.append(taxonLev[i]).append('|');
            //    }
            //}
        } catch (FileNotFoundException e) {
            LOG.error("[SOAPMetas::" + MEPHWrongProfilingMethod.class.getName() + "] mpa taxonomy list file not found. " + e.toString());
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + MEPHWrongProfilingMethod.class.getName() + "] mpa taxonomy list file reader error. " + e.toString());
        }
    }

}

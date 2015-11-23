package iu.pti.hbaseapp;

import java.util.Arrays;
import java.util.LinkedList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

public class Constants {
	// data types
	public enum DataType {INT, STRING, DOUBLE, LONG, UNKNOWN};
	
	/** a dummy field name used with analyzer */
	public static final String DUMMY_FIELD_NAME = "dummyText";
	
	/** the GMT/UTC time zone */
	public static final String TIME_ZONE_GMT = "GMT";
	
	/** max number of KeyValues to get in a batch when scanning table*/
	public static final int TABLE_SCAN_BATCH = 5000;
	
	/** number of query results per output file */
	public static final int RESULTS_PER_FILE = 20000;
	
	/** Lucene analyzer */
	private static Analyzer analyzer = null;
	
	public synchronized static Analyzer getLuceneAnalyzer() {
		if (analyzer != null) {
			return analyzer;
		}		
		analyzer = new StandardAnalyzer(Version.LUCENE_36, getStopWordSet());
		return analyzer;
	}
	
	/** Set of stop words in all languages */
	private static CharArraySet stopWordSet = null;
	
	public synchronized static CharArraySet getStopWordSet() {
		if (stopWordSet != null) {
			return stopWordSet;
		}
		
		LinkedList<String> stopWordList = new LinkedList<String>();
		stopWordList.addAll(Arrays.asList(STOP_WORDS_FRENCH));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_ITALIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_BENGALI));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_ROUMANIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_ARABIC));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_JAPANESE));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_HUGARIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_TURKISH));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_PORTUGUESE));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_BULGARIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_MARATHI));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_CHINESE));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_NORWEGIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_LATVIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_INDONESIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_POLISH));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_SWEDISH));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_KOREAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_DUTCH));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_GERMAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_GREEK));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_RUSSIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_PERSIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_SPANISH));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_HINDI));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_EGNLISH));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_FINNISH));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_DANISH));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_ARMENIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_BRAZILIAN));
		stopWordList.addAll(Arrays.asList(STOP_WORDS_CZECH));
		
		stopWordSet = new CharArraySet(Version.LUCENE_36, stopWordList.size(), false);
		stopWordSet.addAll(stopWordList);
		return stopWordSet;
	}
	
	public static final String[] STOP_WORDS_FRENCH = {
		"au", "ce", "ces", "dans", "de", "des", "du", "elle", "en", "et", "eux", "il", "je", "la", "le", "leur", "lui", "ma", "mais", "m\u00EAme",
		"mes", "moi", "ne", "nos", "notre", "ou", "par", "pas", "qu", "que", "qui", "sa", "se", "ses", "sur", "ta", "te", "tes", "toi", "ton", "tu",
		"un", "une", "vos", "votre", "vous", "c", "d", "j", "l", "\u00E0", "m", "n", "s", "t", "y", "\u00E9t\u00E9", "\u00E9t\u00E9e", 
		"\u00E9t\u00E9es", "\u00E9t\u00E9s", "\u00E9tant", "\u00E9tante", "\u00E9tants", "\u00E9tantes", "suis", "es", "est", "sommes", "\u00EAtes",
		"sont", "seras", "sera", "serons", "serez", "seront", "serais", "serait", "serions", "seriez", "seraient", "\u00E9tais", "\u00E9tait",
		"\u00E9tions", "\u00E9tiez", "\u00E9taient", "fus", "fut", "f\u00FBmes", "f\u00FBtes", "furent", "sois", "soit", "soyons", "soyez", "soient",
		"fusse", "fusses", "f\u00FBt", "fussions", "fussiez", "fussent", "ayant", "ayante", "ayantes", "ayants", "eu", "eue", "eues", "eus", "ai",
		"as", "avons", "avez", "ont", "aurai", "auras", "aura", "aurons", "aurez", "auront", "aurais", "aurait", "aurions", "auriez", "auraient",
		"avais", "avait", "avions", "aviez", "avaient", "eut", "e\u00FBmes", "e\u00FBtes", "eurent", "aie", "aies", "ait", "ayons", "ayez", "aient",
		"eusse", "eusses", "e\u00FBt", "eussions", "eussiez", "eussent"
	};
		
	public static final String[] STOP_WORDS_ITALIAN = {
		"al", "allo", "ai", "agli", "agl", "alla", "alle", "con", "col", "coi", "da", "dal", "dallo", "dai", "dagli", "dall", "dagl", "dalla",
		"dalle", "di", "del", "dello", "dei", "degli", "degl", "della", "delle", "nel", "nello", "nei", "negli", "nell", "negl", "nella", "nelle",
		"su", "sul", "sullo", "sui", "sugli", "sull", "sugl", "sulla", "sulle", "per", "tra", "contro", "io", "tu", "lui", "lei", "noi", "voi",
		"loro", "mio", "mia", "miei", "mie", "tuo", "tua", "tuoi", "tue", "suo", "sua", "suoi", "sue", "nostro", "nostra", "nostri", "nostre",
		"vostro", "vostra", "vostri", "vostre", "mi", "ti", "ci", "vi", "lo", "la", "li", "le", "gli", "ne", "il", "un", "uno", "una", "ma", "ed",
		"se", "perch\u00E9", "anche", "dov", "dove", "che", "chi", "cui", "non", "pi\u00F9", "quale", "quanto", "quanti", "quanta", "quante",
		"quello", "quelli", "quella", "quelle", "questo", "questi", "questa", "queste", "si", "tutto", "tutti", "a", "c", "e", "i", "l", "o", "ho",
		"hai", "ha", "abbiamo", "avete", "hanno", "abbia", "abbiate", "abbiano", "avr\u00F2", "avrai", "avr\u00E0", "avremo", "avrete", "avranno",
		"avrei", "avresti", "avrebbe", "avremmo", "avreste", "avrebbero", "avevo", "avevi", "aveva", "avevamo", "avevate", "avevano", "ebbi",
		"avesti", "ebbe", "avemmo", "aveste", "ebbero", "avessi", "avesse", "avessimo", "avessero", "avendo", "avuto", "avuta", "avuti", "avute",
		"sono", "sei", "\u00E8", "siamo", "siete", "sia", "siate", "siano", "sar\u00F2", "sarai", "sar\u00E0", "saremo", "sarete", "saranno",
		"sarei", "saresti", "sarebbe", "saremmo", "sareste", "sarebbero", "ero", "eri", "era", "eravamo", "eravate", "erano", "fui", "fosti", "fu",
		"fummo", "foste", "furono", "fossi", "fosse", "fossimo", "fossero", "essendo", "faccio", "fai", "facciamo", "fanno", "faccia", "facciate",
		"facciano", "far\u00F2", "farai", "far\u00E0", "faremo", "farete", "faranno", "farei", "faresti", "farebbe", "faremmo", "fareste",
		"farebbero", "facevo", "facevi", "faceva", "facevamo", "facevate", "facevano", "feci", "facesti", "fece", "facemmo", "faceste", "fecero",
		"facessi", "facesse", "facessimo", "facessero", "facendo", "sto", "stai", "sta", "stiamo", "stanno", "stia", "stiate", "stiano",
		"star\u00F2", "starai", "star\u00E0", "staremo", "starete", "staranno", "starei", "staresti", "starebbe", "staremmo", "stareste",
		"starebbero", "stavo", "stavi", "stava", "stavamo", "stavate", "stavano", "stetti", "stesti", "stette", "stemmo", "steste", "stettero",
		"stessi", "stesse", "stessimo", "stessero", "stando"
	};
	
	public static final String[] STOP_WORDS_BENGALI = {
		"\uFEFF\u098F\u0987", "\u0993", "\u09A5\u09C7\u0995\u09C7", "\u0995\u09B0\u09C7", "\u098F", "\u09A8\u09BE", "\u0993\u0987",
		"\u098F\u0995\u09CD", "\u09A8\u09BF\u09DF\u09C7", "\u0995\u09B0\u09BE", "\u09AC\u09B2\u09C7\u09A8", "\u09B8\u0999\u09CD\u0997\u09C7",
		"\u09AF\u09C7", "\u098F\u09AC", "\u09A4\u09BE", "\u0986\u09B0", "\u0995\u09CB\u09A8\u09CB", "\u09AC\u09B2\u09C7", "\u09B8\u09C7\u0987",
		"\u09A6\u09BF\u09A8", "\u09B9\u09DF", "\u0995\u09BF", "\u09A6\u09C1", "\u09AA\u09B0\u09C7", "\u09B8\u09AC", "\u09A6\u09C7\u0993\u09DF\u09BE",
		"\u09AE\u09A7\u09CD\u09AF\u09C7", "\u098F\u09B0", "\u09B8\u09BF", "\u09B6\u09C1\u09B0\u09C1", "\u0995\u09BE\u099C", 
		"\u0995\u09BF\u099B\u09C1", "\u0995\u09BE\u099B\u09C7", "\u09B8\u09C7", "\u09A4\u09AC\u09C7", "\u09AC\u09BE", "\u09AC\u09A8",
		"\u0986\u0997\u09C7", "\u099C\u09CD\u09A8\u099C\u09A8", "\u09AA\u09BF", "\u09AA\u09B0", "\u09A4\u09CB", "\u099B\u09BF\u09B2",
		"\u098F\u0996\u09A8", "\u0986\u09AE\u09B0\u09BE", "\u09AA\u09CD\u09B0\u09BE\u09DF", "\u09A6\u09C1\u0987",
		"\u0986\u09AE\u09BE\u09A6\u09C7\u09B0", "\u09A4\u09BE\u0987", "\u0985\u09A8\u09CD\u09AF", "\u0997\u09BF\u09DF\u09C7",
		"\u09AA\u09CD\u09B0\u09AF\u09A8\u09CD\u09A4", "\u09AE\u09A8\u09C7", "\u09A8\u09A4\u09C1\u09A8", "\u09AE\u09A4\u09CB",
		"\u0995\u09C7\u0996\u09BE", "\u09AA\u09CD\u09B0\u09A5\u09AE", "\u0986\u099C", "\u099F\u09BF", "\u09A7\u09BE\u09AE\u09BE\u09B0",
		"\u0985\u09A8\u09C7\u0995", "\u09AC\u09BF\u09AD\u09BF\u09A8\u09CD\u09A8", "\u09B0", "\u09B9\u09BE\u099C\u09BE\u09B0",
		"\u099C\u09BE\u09A8\u09BE", "\u09A8\u09DF", "\u0985\u09AC\u09B6\u09CD\u09AF", "\u09AC\u09C7\u09B6\u09BF", "\u098F\u09B8",
		"\u0995\u09B0\u09C7", "\u0995\u09C7", "\u09B9\u09A4\u09C7", "\u09AC\u09BF", "\u0995\u09DF\u09C7\u0995", "\u09B8\u09B9", "\u09AC\u09C7\u09B6",
		"\u098F\u09AE\u09A8", "\u098F\u09AE\u09A8\u09BF", "\u0995\u09C7\u09A8", "\u0995\u09C7\u0989", "\u09A8\u09C7\u0993\u09DF\u09BE",
		"\u099A\u09C7\u09B7\u09CD\u099F\u09BE", "\u09B2\u0995\u09CD\u09B7", "\u09AC\u09B2\u09BE", "\u0995\u09BE\u09B0\u09A3", "\u0986\u099B\u09C7",
		"\u09B6\u09C1\u09A7\u09C1", "\u09A4\u0996\u09A8", "\u09AF\u09BE", "\u098F\u09B8\u09C7", "\u099A\u09BE\u09B0", "\u099B\u09BF\u09B2",
		"\u09AF\u09A6\u09BF", "\u0986\u09AC\u09BE\u09B0", "\u0995\u09CB\u099F\u09BF", "\u0989\u09A4\u09CD\u09A4\u09B0",
		"\u09B8\u09BE\u09AE\u09A8\u09C7", "\u0989\u09AA\u09B0", "\u09AC\u0995\u09CD\u09A4\u09AC\u09CD\u09AF", "\u098F\u09A4",
		"\u09AA\u09CD\u09B0\u09BE\u09A5\u09AE\u09BF\u0995", "\u0989\u09AA\u09B0\u09C7", "\u0986\u099B\u09C7", "\u09AA\u09CD\u09B0\u09A4\u09BF",
		"\u0995\u09BE\u099C\u09C7", "\u09AF\u0996\u09A8", "\u0996\u09C1\u09AC", "\u09AC\u09B9\u09C1", "\u0997\u09C7\u09B2",
		"\u09AA\u09C7\u09DF\u09CD\u09B0\u09CD", "\u099A\u09BE\u09B2\u09C1", "\u0987", "\u09A8\u09BE\u0997\u09BE\u09A6", "\u09A5\u09BE\u0995\u09BE",
		"\u09AA\u09BE\u099A", "\u09AF\u09BE\u0993\u09DF\u09BE", "\u09B0\u0995\u09AE", "\u09B8\u09BE\u09A7\u09BE\u09B0\u09A3",
		"\u0995\u09AE\u09A8\u09C7"
	};
		
	public static final String[] STOP_WORDS_ROUMANIAN = {
		"acea", "aceasta", "aceast\u0103", "aceea", "acei", "aceia", "acel", "acela", "acele", "acelea", "acest", "acesta", "aceste", "acestea",
		"ace\u015Fti", "ace\u015Ftia", "acolo", "acum", "ai", "aia", "aib\u0103", "aici", "al", "\u0103la", "ale", "alea", "\u0103lea", "altceva",
		"altcineva", "ar", "a\u015F", "a\u015Fadar", "asemenea", "asta", "\u0103sta", "ast\u0103zi", "astea", "\u0103stea", "\u0103\u015Ftia",
		"asupra", "a\u0163i", "au", "avea", "avem", "ave\u0163i", "azi", "bucur", "bun\u0103", "ca", "c\u0103", "c\u0103ci", "c\u00E2nd",
		"c\u0103rei", "c\u0103ror", "c\u0103rui", "c\u00E2t", "c\u00E2te", "c\u00E2\u0163i", "c\u0103tre", "c\u00E2tva", "caut", "ce", "cel", "ceva",
		"chiar", "cinci", "c\u00EEnd", "cine", "cineva", "c\u00EEt", "c\u00EEte", "c\u00EE\u0163i", "c\u00EEtva", "contra", "cu", "cum", "cumva",
		"cur\u00E2nd", "cur\u00EEnd", "da", "d\u0103", "dac\u0103", "dar", "dat\u0103", "datorit\u0103", "dau", "de", "deci", "deja", "deoarece",
		"departe", "de\u015Fi", "dinaintea", "dintr-", "dintre", "doi", "doilea", "dou\u0103", "drept", "dup\u0103", "ea", "ei", "el", "ele", "eram",
		"este", "e\u015Fti", "eu", "f\u0103r\u0103", "fata", "fi", "fie", "fiecare", "fii", "fim", "fi\u0163i", "fiu", "frumos", "gra\u0163ie",
		"halb\u0103", "iar", "ieri", "\u00EEi", "\u00EEl", "\u00EEmi", "\u00EEmpotriva", "\u00EEn", "\u00EEnainte", "\u00EEnaintea",
		"\u00EEnc\u00E2t", "\u00EEnc\u00EEt", "\u00EEncotro", "\u00EEntre", "\u00EEntruc\u00E2t", "\u00EEntruc\u00EEt", "\u00EE\u0163i", "la",
		"l\u00E2ng\u0103", "le", "li", "l\u00EEng\u0103", "lor", "lui", "m\u0103", "mai", "m\u00E2ine", "mea", "mei", "mele", "mereu", "meu", "mi",
		"mie", "m\u00EEine", "mult", "mult\u0103", "mul\u0163i", "mul\u0163umesc", "ne", "nevoie", "nic\u0103ieri", "nici", "nimeni", "nimeri",
		"nimic", "ni\u015Fte", "noastr\u0103", "noastre", "noi", "noroc", "no\u015Ftri", "nostru", "nou\u0103", "nu", "ori", "oric\u00E2nd",
		"oricare", "oric\u00E2t", "orice", "oric\u00EEnd", "oricine", "oric\u00EEt", "oricum", "oriunde", "p\u00E2n\u0103", "patra", "patru",
		"patrulea", "pe", "pentru", "peste", "p\u00EEn\u0103", "poate", "prea", "prima", "primul", "prin", "printr-", "pu\u0163in", "pu\u0163ina",
		"pu\u0163in\u0103", "rog", "sa", "s\u0103", "s\u0103i", "\u015Fapte", "\u015Fase", "sau", "s\u0103u", "se", "\u015Fi", "s\u00EEnt",
		"s\u00EEntem", "s\u00EEnte\u0163i", "spate", "spre", "\u015Ftiu", "suntem", "sunte\u0163i", "sut\u0103", "ta", "t\u0103i", "tale",
		"t\u0103u", "te", "\u0163i", "\u0163ie", "timp", "toat\u0103", "toate", "to\u0163i", "totu\u015Fi", "trei", "treia", "treilea", "tu",
		"un", "una", "unde", "undeva", "unei", "uneia", "unele", "uneori", "unii", "unor", "unora", "unu", "unui", "unuia", "unul", "v\u0103", "vi",
		"voastr\u0103", "voastre", "voi", "vo\u015Ftri", "vostru", "vou\u0103", "vreme", "vreo", "vreun", "zece", "zi", "zice"
	};
	
	public static final String[] STOP_WORDS_ARABIC = {
		"\u0628", "\u0627", "\u0623", "\u060C", "\u0639\u0634\u0631", "\u0639\u062F\u062F", "\u0639\u062F\u0629", "\u0639\u0634\u0631\u0629",
		"\u0639\u062F\u0645", "\u0639\u0627\u0645", "\u0639\u0627\u0645\u0627", "\u0639\u0646", "\u0639\u0646\u062F",
		"\u0639\u0646\u062F\u0645\u0627", "\u0639\u0644\u0649", "\u0639\u0644\u064A\u0647", "\u0639\u0644\u064A\u0647\u0627",
		"\u0632\u064A\u0627\u0631\u0629", "\u0633\u0646\u0629", "\u0633\u0646\u0648\u0627\u062A", "\u062A\u0645", "\u0636\u062F",
		"\u0628\u0639\u062F", "\u0628\u0639\u0636", "\u0627\u0639\u0627\u062F\u0629", "\u0627\u0639\u0644\u0646\u062A", "\u0628\u0633\u0628\u0628",
		"\u062D\u062A\u0649", "\u0627\u0630\u0627", "\u0627\u062D\u062F", "\u0627\u062B\u0631", "\u0628\u0631\u0633", "\u0628\u0627\u0633\u0645",
		"\u063A\u062F\u0627", "\u0634\u062E\u0635\u0627", "\u0635\u0628\u0627\u062D", "\u0627\u0637\u0627\u0631", "\u0627\u0631\u0628\u0639\u0629",
		"\u0627\u062E\u0631\u0649", "\u0628\u0627\u0646", "\u0627\u062C\u0644", "\u063A\u064A\u0631", "\u0628\u0634\u0643\u0644",
		"\u062D\u0627\u0644\u064A\u0627", "\u0628\u0646", "\u0628\u0647", "\u062B\u0645", "\u0627\u0641", "\u0627\u0646", "\u0627\u0648",
		"\u0627\u064A", "\u0628\u0647\u0627", "\u0635\u0641\u0631", "\u062D\u064A\u062B", "\u0627\u0643\u062F", "\u0627\u0644\u0627",
		"\u0627\u0645\u0627", "\u0627\u0645\u0633", "\u0627\u0644\u0633\u0627\u0628\u0642", "\u0627\u0644\u062A\u0649", "\u0627\u0644\u062A\u064A",
		"\u0627\u0643\u062B\u0631", "\u0627\u064A\u0627\u0631", "\u0627\u064A\u0636\u0627", "\u062B\u0644\u0627\u062B\u0629",
		"\u0627\u0644\u0630\u0627\u062A\u064A", "\u0627\u0644\u0627\u062E\u064A\u0631\u0629", "\u0627\u0644\u062B\u0627\u0646\u064A",
		"\u0627\u0644\u062B\u0627\u0646\u064A\u0629", "\u0627\u0644\u0630\u0649", "\u0627\u0644\u0630\u064A", "\u0627\u0644\u0627\u0646",
		"\u0627\u0645\u0627\u0645", "\u0627\u064A\u0627\u0645", "\u062E\u0644\u0627\u0644", "\u062D\u0648\u0627\u0644\u0649",
		"\u0627\u0644\u0630\u064A\u0646", "\u0627\u0644\u0627\u0648\u0644", "\u0627\u0644\u0627\u0648\u0644\u0649", "\u0628\u064A\u0646",
		"\u0630\u0644\u0643", "\u062F\u0648\u0646", "\u062D\u0648\u0644", "\u062D\u064A\u0646", "\u0627\u0644\u0641", "\u0627\u0644\u0649",
		"\u0627\u0646\u0647", "\u0627\u0648\u0644", "\u0636\u0645\u0646", "\u0627\u0646\u0647\u0627", "\u062C\u0645\u064A\u0639",
		"\u0627\u0644\u0645\u0627\u0636\u064A", "\u0627\u0644\u0648\u0642\u062A", "\u0627\u0644\u0645\u0642\u0628\u0644",
		"\u0627\u0644\u064A\u0648\u0645", "\u0640", "\u0641", "\u0648", "\u06486", "\u0642\u062F", "\u0644\u0627", "\u0645\u0627", "\u0645\u0639",
		"\u0645\u0633\u0627\u0621", "\u0647\u0630\u0627", "\u0648\u0627\u062D\u062F", "\u0648\u0627\u0636\u0627\u0641",
		"\u0648\u0627\u0636\u0627\u0641\u062A", "\u0641\u0627\u0646", "\u0642\u0628\u0644", "\u0642\u0627\u0644", "\u0643\u0627\u0646",
		"\u0644\u062F\u0649", "\u0646\u062D\u0648", "\u0647\u0630\u0647", "\u0648\u0627\u0646", "\u0648\u0627\u0643\u062F",
		"\u0643\u0627\u0646\u062A", "\u0648\u0627\u0648\u0636\u062D", "\u0645\u0627\u064A\u0648", "\u0641\u0649", "\u0641\u064A", "\u0643\u0644",
		"\u0644\u0645", "\u0644\u0646", "\u0644\u0647", "\u0645\u0646", "\u0647\u0648", "\u0647\u064A", "\u0642\u0648\u0629", "\u0643\u0645\u0627",
		"\u0644\u0647\u0627", "\u0645\u0646\u0630", "\u0648\u0642\u062F", "\u0648\u0644\u0627", "\u0646\u0641\u0633\u0647",
		"\u0644\u0642\u0627\u0621", "\u0645\u0642\u0627\u0628\u0644", "\u0647\u0646\u0627\u0643", "\u0648\u0642\u0627\u0644",
		"\u0648\u0643\u0627\u0646", "\u0646\u0647\u0627\u064A\u0629", "\u0648\u0642\u0627\u0644\u062A", "\u0648\u0643\u0627\u0646\u062A",
		"\u0644\u0644\u0627\u0645\u0645", "\u0641\u064A\u0647", "\u0643\u0644\u0645", "\u0644\u0643\u0646", "\u0648\u0641\u064A",
		"\u0648\u0642\u0641", "\u0648\u0644\u0645", "\u0648\u0645\u0646", "\u0648\u0647\u0648", "\u0648\u0647\u064A", "\u064A\u0648\u0645",
		"\u0641\u064A\u0647\u0627", "\u0645\u0646\u0647\u0627", "\u0645\u0644\u064A\u0627\u0631", "\u0644\u0648\u0643\u0627\u0644\u0629",
		"\u064A\u0643\u0648\u0646", "\u064A\u0645\u0643\u0646", "\u0645\u0644\u064A\u0648\u0646"		
	};
	
	public static final String[] STOP_WORDS_JAPANESE = {
		"\u3053\u308C", "\u305D\u308C", "\u3042\u308C", "\u3053\u306E", "\u305D\u306E", "\u3042\u306E", "\u3053\u3053", "\u305D\u3053",
		"\u3042\u305D\u3053", "\u3053\u3061\u3089", "\u3069\u3053", "\u3060\u308C", "\u306A\u306B", "\u306A\u3093", "\u4F55", "\u79C1",
		"\u8CB4\u65B9", "\u8CB4\u65B9\u65B9", "\u6211\u3005", "\u79C1\u9054", "\u3042\u306E\u4EBA", "\u3042\u306E\u304B\u305F", "\u5F7C\u5973",
		"\u5F7C", "\u3067\u3059", "\u3042\u308A\u307E\u3059", "\u304A\u308A\u307E\u3059", "\u3044\u307E\u3059", "\u306F", "\u304C", "\u306E",
		"\u306B", "\u3092", "\u3067", "\u3048", "\u304B\u3089", "\u307E\u3067", "\u3088\u308A", "\u3082", "\u3069\u306E", "\u3068", "\u3057",
		"\u305D\u308C\u3067", "\u3057\u304B\u3057"		
	};
	
	public static final String[] STOP_WORDS_HUGARIAN = {
		"ahogy", "ahol", "aki", "akik", "akkor", "alatt", "\u00E1ltal", "\u00E1ltal\u00E1ban", "amely", "amelyek", "amelyekben", "amelyeket",
		"amelyet", "amelynek", "ami", "amit", "amolyan", "am\u00EDg", "amikor", "\u00E1t", "abban", "ahhoz", "annak", "arra", "arr\u00F3l", "az",
		"azok", "azon", "azt", "azzal", "az\u00E9rt", "azt\u00E1n", "azut\u00E1n", "azonban", "b\u00E1r", "bel\u00FCl", "cikk", "cikkek", "cikkeket",
		"csak", "de", "e", "eddig", "eg\u00E9sz", "egy", "egyes", "egyetlen", "egy\u00E9b", "egyik", "egyre", "ekkor", "el", "el\u00E9g", "ellen",
		"el\u00F5", "el\u00F5sz\u00F6r", "el\u00F5tt", "els\u00F5", "\u00E9n", "\u00E9ppen", "ebben", "ehhez", "emilyen", "ennek", "erre", "ez",
		"ezt", "ezek", "ezen", "ezzel", "ez\u00E9rt", "\u00E9s", "fel", "fel\u00E9", "hanem", "hiszen", "hogy", "hogyan", "igen", "\u00EDgy",
		"illetve", "ilyen", "ilyenkor", "ison", "ism\u00E9t", "itt", "j\u00F3", "j\u00F3l", "jobban", "kell", "kellett", "kereszt\u00FCl",
		"keress\u00FCnk", "ki", "k\u00EDv\u00FCl", "k\u00F6z\u00F6tt", "k\u00F6z\u00FCl", "legal\u00E1bb", "lehet", "lehetett", "legyen", "lenne",
		"lenni", "lesz", "lett", "maga", "mag\u00E1t", "majd", "majd", "m\u00E1r", "m\u00E1s", "m\u00E1sik", "meg", "m\u00E9g", "mellett", "mert",
		"mely", "melyek", "mi", "mit", "m\u00EDg", "mi\u00E9rt", "milyen", "mikor", "minden", "mindent", "mindenki", "mindig", "mintha", "mivel",
		"nagy", "nagyobb", "nagyon", "ne", "n\u00E9ha", "nekem", "neki", "nem", "n\u00E9h\u00E1ny", "n\u00E9lk\u00FCl", "nincs", "olyan", "ott",
		"\u00F6ssze", "\u00F5", "\u00F5k", "\u00F5ket", "pedig", "persze", "r\u00E1", "s", "saj\u00E1t", "sem", "semmi", "sok", "sokat", "sokkal",
		"sz\u00E1m\u00E1ra", "szemben", "szerint", "szinte", "tal\u00E1n", "teh\u00E1t", "teljes", "tov\u00E1bb", "tov\u00E1bb\u00E1", "t\u00F6bb",
		"\u00FAgy", "ugyanis", "\u00FAj", "\u00FAjabb", "\u00FAjra", "ut\u00E1n", "ut\u00E1na", "utols\u00F3", "vagy", "vagyis", "valaki", "valami",
		"valamint", "val\u00F3", "vagyok", "vannak", "voltam", "voltak", "voltunk", "vissza", "vele", "viszont", "volna"
	};
	
	public static final String[] STOP_WORDS_TURKISH = {
		"acaba", "ama", "asl\u0131nda", "az", "baz\u0131", "belki", "biri", "birka\u00E7", "bir\u015Fey", "biz", "bu", "\u00E7ok",
		"\u00E7\u00FCnk\u00FC", "da", "daha", "de", "defa", "diye", "e\u011Fer", "en", "gibi", "hepsi", "hi\u00E7", "i\u00E7in", "ile", "ise", "kez",
		"ki", "kim", "m\u0131", "mu", "m\u00FC", "nas\u0131l", "ne", "neden", "nerde", "nerede", "nereye", "ni\u00E7in", "niye", "o", "sanki",
		"\u015Fey", "siz", "\u015Fu", "t\u00FCm", "ve", "veya", "ya", "yani"
	};
	
	public static final String[] STOP_WORDS_PORTUGUESE = {
		"de", "a", "o", "que", "e", "da", "em", "um", "para", "com", "n\u00E3o", "uma", "se", "na", "por", "mais", "as", "dos", "como", "mas", "ao",
		"ele", "das", "\u00E0", "seu", "sua", "ou", "quando", "muito", "nos", "j\u00E1", "eu", "tamb\u00E9m", "s\u00F3", "pelo", "pela", "at\u00E9",
		"isso", "ela", "entre", "depois", "sem", "mesmo", "aos", "seus", "quem", "nas", "esse", "eles", "voc\u00EA", "essa", "num", "nem", "suas",
		"meu", "\u00E0s", "minha", "numa", "pelos", "elas", "qual", "n\u00F3s", "lhe", "deles", "essas", "esses", "pelas", "este", "dele", "tu",
		"te", "voc\u00EAs", "vos", "lhes", "meus", "minhas", "teu", "tua", "teus", "tuas", "nosso", "nossa", "nossos", "nossas", "dela", "delas",
		"esta", "estes", "estas", "aquele", "aquela", "aqueles", "aquelas", "isto", "aquilo", "estou", "est\u00E1", "estamos", "est\u00E3o",
		"estive", "esteve", "estivemos", "estiveram", "estava", "est\u00E1vamos", "estavam", "estivera", "estiv\u00E9ramos", "esteja", "estejamos",
		"estejam", "estivesse", "estiv\u00E9ssemos", "estivessem", "estiver", "estivermos", "estiverem", "hei", "h\u00E1", "havemos", "h\u00E3o",
		"houve", "houvemos", "houveram", "houvera", "houv\u00E9ramos", "haja", "hajamos", "hajam", "houvesse", "houv\u00E9ssemos", "houvessem",
		"houver", "houvermos", "houverem", "houverei", "houver\u00E1", "houveremos", "houver\u00E3o", "houveria", "houver\u00EDamos", "houveriam",
		"sou", "somos", "s\u00E3o", "era", "\u00E9ramos", "eram", "fui", "foi", "fomos", "foram", "fora", "f\u00F4ramos", "seja", "sejamos", "sejam",
		"fosse", "f\u00F4ssemos", "fossem", "formos", "forem", "serei", "ser\u00E1", "seremos", "ser\u00E3o", "seria", "ser\u00EDamos", "seriam",
		"tenho", "tem", "temos", "t\u00E9m", "tinha", "t\u00EDnhamos", "tinham", "tive", "teve", "tivemos", "tiveram", "tivera", "tiv\u00E9ramos",
		"tenha", "tenhamos", "tenham", "tivesse", "tiv\u00E9ssemos", "tivessem", "tiver", "tivermos", "tiverem", "terei", "ter\u00E1", "teremos",
		"ter\u00E3o", "teria", "ter\u00EDamos", "teriam"
	};
	
	public static final String[] STOP_WORDS_BULGARIAN = {
		"\u0430", "\u0430\u0432\u0442\u0435\u043D\u0442\u0438\u0447\u0435\u043D", "\u0430\u0437", "\u0430\u043A\u043E", "\u0430\u043B\u0430",
		"\u0431\u0435", "\u0431\u0435\u0437", "\u0431\u0435\u0448\u0435", "\u0431\u0438", "\u0431\u0438\u0432\u0448",
		"\u0431\u0438\u0432\u0448\u0430", "\u0431\u0438\u0432\u0448\u043E", "\u0431\u0438\u043B", "\u0431\u0438\u043B\u0430",
		"\u0431\u0438\u043B\u0438", "\u0431\u0438\u043B\u043E", "\u0431\u043B\u0430\u0433\u043E\u0434\u0430\u0440\u044F",
		"\u0431\u043B\u0438\u0437\u043E", "\u0431\u044A\u0434\u0430\u0442", "\u0431\u044A\u0434\u0435", "\u0431\u044F\u0445\u0430",
		"\u0432", "\u0432\u0430\u0441", "\u0432\u0430\u0448", "\u0432\u0430\u0448\u0430", "\u0432\u0435\u0440\u043E\u044F\u0442\u043D\u043E",
		"\u0432\u0435\u0447\u0435", "\u0432\u0437\u0435\u043C\u0430", "\u0432\u0438", "\u0432\u0438\u0435", "\u0432\u0438\u043D\u0430\u0433\u0438",
		"\u0432\u043D\u0438\u043C\u0430\u0432\u0430", "\u0432\u0440\u0435\u043C\u0435", "\u0432\u0441\u0435", "\u0432\u0441\u0435\u043A\u0438",
		"\u0432\u0441\u0438\u0447\u043A\u0438", "\u0432\u0441\u0438\u0447\u043A\u043E", "\u0432\u0441\u044F\u043A\u0430", "\u0432\u044A\u0432",
		"\u0432\u044A\u043F\u0440\u0435\u043A\u0438", "\u0432\u044A\u0440\u0445\u0443", "\u0433", "\u0433\u0438",
		"\u0433\u043B\u0430\u0432\u0435\u043D", "\u0433\u043B\u0430\u0432\u043D\u0430", "\u0433\u043B\u0430\u0432\u043D\u043E",
		"\u0433\u043B\u0430\u0441", "\u0433\u043E", "\u0433\u043E\u0434\u0438\u043D\u0430", "\u0433\u043E\u0434\u0438\u043D\u0438",
		"\u0433\u043E\u0434\u0438\u0448\u0435\u043D", "\u0434", "\u0434\u0430", "\u0434\u0430\u043B\u0438", "\u0434\u0432\u0430",
		"\u0434\u0432\u0430\u043C\u0430", "\u0434\u0432\u0430\u043C\u0430\u0442\u0430", "\u0434\u0432\u0435", "\u0434\u0432\u0435\u0442\u0435",
		"\u0434\u0435\u043D", "\u0434\u043D\u0435\u0441", "\u0434\u043D\u0438", "\u0434\u043E", "\u0434\u043E\u0431\u0440\u0430",
		"\u0434\u043E\u0431\u0440\u0435", "\u0434\u043E\u0431\u0440\u043E", "\u0434\u043E\u0431\u044A\u0440", "\u0434\u043E\u043A\u0430\u0442\u043E",
		"\u0434\u043E\u043A\u043E\u0433\u0430", "\u0434\u043E\u0440\u0438", "\u0434\u043E\u0441\u0435\u0433\u0430", "\u0434\u043E\u0441\u0442\u0430",
		"\u0434\u0440\u0443\u0433", "\u0434\u0440\u0443\u0433\u0430", "\u0434\u0440\u0443\u0433\u0438", "\u0435", "\u0435\u0432\u0442\u0438\u043D",
		"\u0435\u0434\u0432\u0430", "\u0435\u0434\u0438\u043D", "\u0435\u0434\u043D\u0430", "\u0435\u0434\u043D\u0430\u043A\u0432\u0430",
		"\u0435\u0434\u043D\u0430\u043A\u0432\u0438", "\u0435\u0434\u043D\u0430\u043A\u044A\u0432", "\u0435\u0434\u043D\u043E",
		"\u0435\u043A\u0438\u043F", "\u0435\u0442\u043E", "\u0436\u0438\u0432\u043E\u0442", "\u0437\u0430",
		"\u0437\u0430\u0431\u0430\u0432\u044F\u043C", "\u0437\u0430\u0434", "\u0437\u0430\u0435\u0434\u043D\u043E",
		"\u0437\u0430\u0440\u0430\u0434\u0438", "\u0437\u0430\u0441\u0435\u0433\u0430", "\u0437\u0430\u0441\u043F\u0430\u043B",
		"\u0437\u0430\u0442\u043E\u0432\u0430", "\u0437\u0430\u0449\u043E", "\u0437\u0430\u0449\u043E\u0442\u043E", "\u0438", "\u0438\u0437",
		"\u0438\u043B\u0438", "\u0438\u043C", "\u0438\u043C\u0430", "\u0438\u043C\u0430\u0442", "\u0438\u0441\u043A\u0430", "\u0439",
		"\u043A\u0430\u0437\u0430", "\u043A\u0430\u043A", "\u043A\u0430\u043A\u0432\u0430", "\u043A\u0430\u043A\u0432\u043E",
		"\u043A\u0430\u043A\u0442\u043E", "\u043A\u0430\u043A\u044A\u0432", "\u043A\u0430\u0442\u043E", "\u043A\u043E\u0433\u0430",
		"\u043A\u043E\u0433\u0430\u0442\u043E", "\u043A\u043E\u0435\u0442\u043E", "\u043A\u043E\u0438\u0442\u043E", "\u043A\u043E\u0439",
		"\u043A\u043E\u0439\u0442\u043E", "\u043A\u043E\u043B\u043A\u043E", "\u043A\u043E\u044F\u0442\u043E", "\u043A\u044A\u0434\u0435",
		"\u043A\u044A\u0434\u0435\u0442\u043E", "\u043A\u044A\u043C", "\u043B\u0435\u0441\u0435\u043D", "\u043B\u0435\u0441\u043D\u043E",
		"\u043B\u0438", "\u043B\u043E\u0448", "\u043C", "\u043C\u0430\u0439", "\u043C\u0430\u043B\u043A\u043E", "\u043C\u0435",
		"\u043C\u0435\u0436\u0434\u0443", "\u043C\u0435\u043A", "\u043C\u0435\u043D", "\u043C\u0435\u0441\u0435\u0446", "\u043C\u0438",
		"\u043C\u043D\u043E\u0433\u043E", "\u043C\u043D\u043E\u0437\u0438\u043D\u0430", "\u043C\u043E\u0433\u0430", "\u043C\u043E\u0433\u0430\u0442",
		"\u043C\u043E\u0436\u0435", "\u043C\u043E\u043A\u044A\u0440", "\u043C\u043E\u043B\u044F", "\u043C\u043E\u043C\u0435\u043D\u0442\u0430",
		"\u043C\u0443", "\u043D", "\u043D\u0430", "\u043D\u0430\u0434", "\u043D\u0430\u0437\u0430\u0434", "\u043D\u0430\u0439",
		"\u043D\u0430\u043F\u0440\u0430\u0432\u0438", "\u043D\u0430\u043F\u0440\u0435\u0434", "\u043D\u0430\u043F\u0440\u0438\u043C\u0435\u0440",
		"\u043D\u0430\u0441", "\u043D\u0435", "\u043D\u0435\u0433\u043E", "\u043D\u0435\u0449\u043E", "\u043D\u0435\u044F", "\u043D\u0438",
		"\u043D\u0438\u0435", "\u043D\u0438\u043A\u043E\u0439", "\u043D\u0438\u0442\u043E", "\u043D\u0438\u0449\u043E", "\u043D\u043E",
		"\u043D\u043E\u0432", "\u043D\u043E\u0432\u0430", "\u043D\u043E\u0432\u0438", "\u043D\u043E\u0432\u0438\u043D\u0430",
		"\u043D\u044F\u043A\u043E\u0438", "\u043D\u044F\u043A\u043E\u0439", "\u043D\u044F\u043A\u043E\u043B\u043A\u043E", "\u043D\u044F\u043C\u0430",
		"\u043E\u0431\u0430\u0447\u0435", "\u043E\u043A\u043E\u043B\u043E", "\u043E\u0441\u0432\u0435\u043D",
		"\u043E\u0441\u043E\u0431\u0435\u043D\u043E", "\u043E\u0442", "\u043E\u0442\u0433\u043E\u0440\u0435", "\u043E\u0442\u043D\u043E\u0432\u043E",
		"\u043E\u0449\u0435", "\u043F\u0430\u043A", "\u043F\u043E", "\u043F\u043E\u0432\u0435\u0447\u0435",
		"\u043F\u043E\u0432\u0435\u0447\u0435\u0442\u043E", "\u043F\u043E\u0434", "\u043F\u043E\u043D\u0435", "\u043F\u043E\u0440\u0430\u0434\u0438",
		"\u043F\u043E\u0441\u043B\u0435", "\u043F\u043E\u0447\u0442\u0438", "\u043F\u0440\u0430\u0432\u0438", "\u043F\u0440\u0435\u0434",
		"\u043F\u0440\u0435\u0434\u0438", "\u043F\u0440\u0435\u0437", "\u043F\u0440\u0438", "\u043F\u044A\u043A",
		"\u043F\u044A\u0440\u0432\u0430\u0442\u0430", "\u043F\u044A\u0440\u0432\u0438", "\u043F\u044A\u0440\u0432\u043E", "\u043F\u044A\u0442\u0438",
		"\u0440\u0430\u0432\u0435\u043D", "\u0440\u0430\u0432\u043D\u0430", "\u0441", "\u0441\u0430", "\u0441\u0430\u043C",
		"\u0441\u0430\u043C\u043E", "\u0441\u0435", "\u0441\u0435\u0433\u0430", "\u0441\u0438", "\u0441\u0438\u043D",
		"\u0441\u043A\u043E\u0440\u043E", "\u0441\u043B\u0435\u0434", "\u0441\u043B\u0435\u0434\u0432\u0430\u0449", "\u0441\u043C\u0435",
		"\u0441\u043C\u044F\u0445", "\u0441\u043F\u043E\u0440\u0435\u0434", "\u0441\u0440\u0435\u0434", "\u0441\u0440\u0435\u0449\u0443",
		"\u0441\u0442\u0435", "\u0441\u044A\u043C", "\u0441\u044A\u0441", "\u0441\u044A\u0449\u043E", "\u0442", "\u0442\u0430\u0437\u0438",
		"\u0442\u0430\u043A\u0430", "\u0442\u0430\u043A\u0438\u0432\u0430", "\u0442\u0430\u043A\u044A\u0432", "\u0442\u0430\u043C",
		"\u0442\u0432\u043E\u0439", "\u0442\u0435", "\u0442\u0435\u0437\u0438", "\u0442\u0438", "\u0442.\u043D.", "\u0442\u043E",
		"\u0442\u043E\u0432\u0430", "\u0442\u043E\u0433\u0430\u0432\u0430", "\u0442\u043E\u0437\u0438", "\u0442\u043E\u0439",
		"\u0442\u043E\u043B\u043A\u043E\u0432\u0430", "\u0442\u043E\u0447\u043D\u043E", "\u0442\u0440\u0438", "\u0442\u0440\u044F\u0431\u0432\u0430",
		"\u0442\u0443\u043A", "\u0442\u044A\u0439", "\u0442\u044F", "\u0442\u044F\u0445", "\u0443", "\u0443\u0442\u0440\u0435",
		"\u0445\u0430\u0440\u0435\u0441\u0432\u0430", "\u0445\u0438\u043B\u044F\u0434\u0438", "\u0447", "\u0447\u0430\u0441\u0430", "\u0447\u0435",
		"\u0447\u0435\u0441\u0442\u043E", "\u0447\u0440\u0435\u0437", "\u0449\u0435", "\u0449\u043E\u043C", "\u044E\u043C\u0440\u0443\u043A",
		"\u044F", "\u044F\u043A" 
	};
	
	public static final String[] STOP_WORDS_MARATHI = {
		"\u0906\u0939\u0947", "\u092F\u093E", "\u0906\u0923\u093F", "\u0935", "\u0928\u093E\u0939\u0940", "\u0906\u0939\u0947\u0924",
		"\u092F\u093E\u0928\u0940", "\u0939\u0947", "\u0924\u0930", "\u0924\u0947", "\u0905\u0938\u0947", "\u0939\u094B\u0924\u0947",
		"\u0915\u0947\u0932\u0940", "\u0939\u093E", "\u0939\u0940", "\u092A\u0923", "\u0915\u0930\u0923\u092F\u093E\u0924",
		"\u0915\u093E\u0939\u0940", "\u0915\u0947\u0932\u0947", "\u090F\u0915", "\u0915\u0947\u0932\u093E", "\u0905\u0936\u0940",
		"\u092E\u093E\u0924\u094D\u0930", "\u0924\u094D\u092F\u093E\u0928\u0940", "\u0938\u0941\u0930\u0942", "\u0915\u0930\u0942\u0928",
		"\u0939\u094B\u0924\u0940", "\u0905\u0938\u0942\u0928", "\u0906\u0932\u0947", "\u0924\u094D\u092F\u093E\u092E\u0941\u0933\u0947",
		"\u091D\u093E\u0932\u0940", "\u0939\u094B\u0924\u093E", "\u0926\u094B\u0928", "\u091D\u093E\u0932\u0947", "\u092E\u0941\u092C\u0940",
		"\u0939\u094B\u0924", "\u0924\u094D\u092F\u093E", "\u0906\u0924\u093E", "\u0905\u0938\u093E", "\u092F\u093E\u091A\u094D\u092F\u093E",
		"\u0924\u094D\u092F\u093E\u091A\u094D\u092F\u093E", "\u0924\u093E", "\u0906\u0932\u0940", "\u0915\u0940", "\u092A\u092E", "\u0924\u094B",
		"\u091D\u093E\u0932\u093E", "\u0924\u094D\u0930\u0940", "\u0924\u0930\u0940", "\u092E\u094D\u0939\u0923\u0942\u0928",
		"\u0924\u094D\u092F\u093E\u0928\u093E", "\u0905\u0928\u0947\u0915", "\u0915\u093E\u092E", "\u092E\u093E\u0939\u093F\u0924\u0940",
		"\u0939\u091C\u093E\u0930", "\u0938\u093E\u0917\u093F\u0924\u094D\u0932\u0947", "\u0926\u093F\u0932\u0940", "\u0906\u0932\u093E",
		"\u0906\u091C", "\u0924\u0940", "\u0924\u0938\u0947\u091A", "\u090F\u0915\u093E", "\u092F\u093E\u091A\u0940",
		"\u092F\u0947\u0925\u0940\u0932", "\u0938\u0930\u094D\u0935", "\u0928", "\u0921\u0949", "\u0924\u0940\u0928", "\u092F\u0947\u0925\u0947",
		"\u092A\u093E\u091F\u0940\u0932", "\u0905\u0938\u0932\u092F\u093E\u091A\u0947", "\u0924\u094D\u092F\u093E\u091A\u0940", "\u0915\u093E\u092F",
		"\u0906\u092A\u0932\u094D\u092F\u093E", "\u092E\u094D\u0939\u0923\u091C\u0947", "\u092F\u093E\u0928\u093E",
		"\u092E\u094D\u0939\u0923\u093E\u0932\u0947", "\u0924\u094D\u092F\u093E\u091A\u093E", "\u0905\u0938\u0932\u0947\u0932\u094D\u092F\u093E",
		"\u092E\u0940", "\u0917\u0947\u0932\u094D\u092F\u093E", "\u092F\u093E\u091A\u093E", "\u092F\u0947\u0924", "\u092E", "\u0932\u093E\u0916",
		"\u0915\u092E\u0940", "\u091C\u093E\u0924", "\u091F\u093E", "\u0939\u094B\u0923\u093E\u0930", "\u0915\u093F\u0935\u093E", "\u0915\u093E",
		"\u0905\u0927\u093F\u0915", "\u0918\u0947\u090A\u0928", "\u092A\u0930\u092F\u0924\u0928", "\u0915\u094B\u091F\u0940",
		"\u091D\u093E\u0932\u0947\u0932\u094D\u092F\u093E", "\u0928\u093F\u0930\u094D\u0923\u094D\u092F", "\u092F\u0947\u0923\u093E\u0930",
		"\u0935\u094D\u092F\u0915\u0924"
	};
	
	public static final String[] STOP_WORDS_CHINESE = {
		"\u3001", "\u3002", "\u201C", "\u201D", "\u300A", "\u300B", "\uFF01", "\uFF0C", "\uFF1A", "\uFF1B", "\uFF1F", "\u7684", "\u4E00", "\u4E0D",
		"\u5728", "\u4EBA", "\u6709", "\u662F", "\u4E3A", "\u4EE5", "\u4E8E", "\u4E0A", "\u4ED6", "\u800C", "\u540E", "\u4E4B", "\u6765", "\u53CA",
		"\u4E86", "\u56E0", "\u4E0B", "\u53EF", "\u5230", "\u7531", "\u8FD9", "\u4E0E", "\u4E5F", "\u6B64", "\u4F46", "\u5E76", "\u4E2A", "\u5176",
		"\u5DF2", "\u65E0", "\u5C0F", "\u6211", "\u4EEC", "\u8D77", "\u6700", "\u518D", "\u4ECA", "\u53BB", "\u597D", "\u53EA", "\u53C8", "\u6216",
		"\u5F88", "\u4EA6", "\u67D0", "\u628A", "\u90A3", "\u4F60", "\u4E43", "\u5B83", "\u5427", "\u88AB", "\u6BD4", "\u522B", "\u8D81", "\u5F53",
		"\u4ECE", "\u5230", "\u5F97", "\u6253", "\u51E1", "\u513F", "\u5C14", "\u8BE5", "\u5404", "\u7ED9", "\u8DDF", "\u548C", "\u4F55", "\u8FD8",
		"\u5373", "\u51E0", "\u65E2", "\u770B", "\u636E", "\u8DDD", "\u9760", "\u5566", "\u4E86", "\u53E6", "\u4E48", "\u6BCF", "\u4EEC", "\u561B",
		"\u62FF", "\u54EA", "\u90A3", "\u60A8", "\u51ED", "\u4E14", "\u5374", "\u8BA9", "\u4ECD", "\u5565", "\u5982", "\u82E5", "\u4F7F", "\u8C01",
		"\u867D", "\u968F", "\u540C", "\u6240", "\u5979", "\u54C7", "\u55E1", "\u5F80", "\u54EA", "\u4E9B", "\u5411", "\u6CBF", "\u54DF", "\u7528",
		"\u4E8E", "\u54B1", "\u5219", "\u600E", "\u66FE", "\u81F3", "\u81F4", "\u7740", "\u8BF8", "\u81EA"
	};
	
	public static final String[] STOP_WORDS_NORWEGIAN = {
		"og", "i", "jeg", "det", "en", "et", "til", "er", "som", "p\u00E5", "de", "med", "av", "ikke", "ikkje", "der", "s\u00E5", "var", "meg", "seg",
		"ett", "har", "om", "vi", "mitt", "ha", "hadde", "n\u00E5", "da", "ved", "fra", "du", "ut", "dem", "oss", "opp", "hans", "hvor", "eller",
		"hva", "skal", "selv", "sj\u00F8l", "alle", "vil", "bli", "ble", "blei", "blitt", "kunne", "n\u00E5r", "v\u00E6re", "kom", "noen", "noe",
		"ville", "dere", "som", "deres", "kun", "ja", "etter", "ned", "skulle", "denne", "for", "deg", "si", "sitt", "mot", "\u00E5", "meget",
		"hvorfor", "dette", "disse", "uten", "hvordan", "ingen", "ditt", "blir", "samme", "hvilken", "hvilke", "s\u00E5nn", "inni", "mellom",
		"v\u00E5r", "hver", "hvem", "vors", "hvis", "b\u00E5de", "bare", "enn", "fordi", "f\u00F8r", "mange", "ogs\u00E5", "slik", "v\u00E6rt",
		"v\u00E6re", "b\u00E5e", "begge", "siden", "dykk", "dykkar", "dei", "deira", "deires", "deim", "di", "d\u00E5", "eg", "ein", "eit", "eitt",
		"elles", "honom", "hj\u00E5", "ho", "hoe", "henne", "hennar", "hennes", "hoss", "hossen", "ikkje", "ingi", "inkje", "korleis", "korso",
		"kva", "kvar", "kvarhelst", "kven", "kvi", "kvifor", "medan", "mi", "mykje", "nokon", "noka", "nokor", "noko", "nokre", "si", "sia", "sidan",
		"somt", "somme", "um", "upp", "vere", "vore", "verte", "vort", "varte", "vart" 
	};
	
	public static final String[] STOP_WORDS_LATVIAN = {
		"aiz", "ap", "ar", "apak\u0161", "\u0101rpus", "aug\u0161pus", "bez", "caur", "d\u0113\u013C", "gar", "iek\u0161", "iz", "kop\u0161",
		"labad", "lejpus", "l\u012Bdz", "otrpus", "pa", "par", "p\u0101r", "p\u0113c", "pirms", "pret", "priek\u0161", "starp", "\u0161aipus", "uz",
		"vi\u0146pus", "virs", "virspus", "zem", "apak\u0161pus", "un", "bet", "jo", "ja", "ka", "lai", "tom\u0113r", "tikko", "turpret\u012B",
		"ar\u012B", "kaut", "gan", "t\u0101d\u0113\u013C", "t\u0101", "ne", "tikvien", "vien", "k\u0101", "ir", "te", "vai", "kam\u0113r", "ar",
		"diezin", "dro\u0161i", "diem\u017E\u0113l", "neb\u016Bt", "ik", "it", "ta\u010Du", "nu", "pat", "tiklab", "iek\u0161pus", "nedz", "tik",
		"nevis", "turpretim", "jeb", "iekam", "iek\u0101m", "iek\u0101ms", "kol\u012Bdz", "l\u012Bdzko", "tikl\u012Bdz", "jeb\u0161u", "t\u0101lab",
		"t\u0101p\u0113c", "nek\u0101", "itin", "j\u0101", "jau", "jel", "n\u0113", "nezin", "tad", "tikai", "vis", "tak", "iekams", "vien",
		"b\u016Bt", "biju", "biji", "bija", "bij\u0101m", "bij\u0101t", "esmu", "esi", "esam", "esat", "b\u016B\u0161u", "b\u016Bsi", "b\u016Bs",
		"b\u016Bsim", "b\u016Bsiet", "tikt", "tiku", "tiki", "tika", "tik\u0101m", "tik\u0101t", "tieku", "tiec", "tiek", "tiekam", "tiekat",
		"tik\u0161u", "tiks", "tiksim", "tiksiet", "tapt", "tapi", "tap\u0101t", "topat", "tap\u0161u", "tapsi", "taps", "tapsim", "tapsiet",
		"k\u013C\u016Bt", "k\u013Cuvu", "k\u013Cuvi", "k\u013Cuva", "k\u013Cuv\u0101m", "k\u013Cuv\u0101t", "k\u013C\u016Bstu", "k\u013C\u016Bsti",
		"k\u013C\u016Bst", "k\u013C\u016Bstam", "k\u013C\u016Bstat", "k\u013C\u016B\u0161u", "k\u013C\u016Bsi", "k\u013C\u016Bs", "k\u013C\u016Bsim",
		"k\u013C\u016Bsiet", "var\u0113t", "var\u0113ju", "var\u0113j\u0101m", "var\u0113\u0161u", "var\u0113sim", "var", "var\u0113ji",
		"var\u0113j\u0101t", "var\u0113si", "var\u0113siet", "varat", "var\u0113ja", "var\u0113s" 
	};
	
	public static final String[] STOP_WORDS_INDONESIAN = {
		"ada", "adanya", "adalah", "adapun", "agak", "agaknya", "agar", "akan", "akankah", "akhirnya", "aku", "akulah", "amat", "amatlah", "anda",
		"andalah", "antar", "diantaranya", "antara", "antaranya", "diantara", "apa", "apaan", "mengapa", "apabila", "apakah", "apalagi", "apatah",
		"atau", "ataukah", "ataupun", "bagai", "bagaikan", "sebagai", "sebagainya", "bagaimana", "bagaimanapun", "sebagaimana", "bagaimanakah",
		"bagi", "bahkan", "bahwa", "bahwasanya", "sebaliknya", "banyak", "sebanyak", "beberapa", "seberapa", "begini", "beginian", "beginikah",
		"beginilah", "sebegini", "begitu", "begitukah", "begitulah", "begitupun", "sebegitu", "belum", "belumlah", "sebelum", "sebelumnya",
		"sebenarnya", "berapa", "berapakah", "berapalah", "berapapun", "betulkah", "sebetulnya", "biasa", "biasanya", "bila", "bilakah", "bisa",
		"bisakah", "sebisanya", "boleh", "bolehkah", "bolehlah", "buat", "bukan", "bukankah", "bukanlah", "bukannya", "cuma", "percuma", "dahulu",
		"dalam", "dapat", "dari", "daripada", "dekat", "demi", "demikian", "demikianlah", "sedemikian", "dengan", "depan", "di", "dia", "dialah",
		"dini", "diri", "dirinya", "terdiri", "dong", "dulu", "enggak", "enggaknya", "entah", "entahlah", "terhadap", "terhadapnya", "hal", "hampir",
		"hanya", "hanyalah", "harus", "haruslah", "harusnya", "seharusnya", "hendak", "hendaklah", "hendaknya", "hingga", "sehingga", "ia", "ialah",
		"ibarat", "ingin", "inginkah", "inginkan", "ini", "inikah", "inilah", "itu", "itukah", "itulah", "jangan", "jangankan", "janganlah", "jika",
		"jikalau", "juga", "justru", "kala", "kalau", "kalaulah", "kalaupun", "kalian", "kami", "kamilah", "kamu", "kamulah", "kan", "kapan",
		"kapankah", "kapanpun", "dikarenakan", "karena", "karenanya", "ke", "kecil", "kemudian", "kenapa", "kepada", "kepadanya", "ketika",
		"seketika", "khususnya", "kini", "kinilah", "kiranya", "sekiranya", "kita", "kitalah", "kok", "lagi", "lagian", "selagi", "lah", "lain",
		"lainnya", "melainkan", "selaku", "lalu", "melalui", "terlalu", "lama", "lamanya", "selama", "selamanya", "lebih", "terlebih", "bermacam",
		"macam", "semacam", "maka", "makanya", "makin", "malah", "malahan", "mampu", "mampukah", "mana", "manakala", "manalagi", "masih", "masihkah",
		"semasih", "masing", "mau", "maupun", "semaunya", "memang", "mereka", "merekalah", "meski", "meskipun", "semula", "mungkin", "mungkinkah",
		"nah", "namun", "nanti", "nantinya", "nyaris", "oleh", "olehnya", "seorang", "seseorang", "pada", "padanya", "padahal", "paling",
		"sepanjang", "pantas", "sepantasnya", "sepantasnyalah", "para", "pasti", "pastilah", "per", "pernah", "pula", "pun", "merupakan", "rupanya",
		"serupa", "saat", "saatnya", "sesaat", "saja", "sajalah", "saling", "bersama", "sama", "sesama", "sambil", "sampai", "sana", "sangat",
		"sangatlah", "saya", "sayalah", "se", "sebab", "sebabnya", "sebuah", "tersebut", "tersebutlah", "sedang", "sedangkan", "sedikit",
		"sedikitnya", "segala", "segalanya", "segera", "sesegera", "sejak", "sejenak", "sekali", "sekalian", "sekalipun", "sesekali", "sekaligus",
		"sekarang", "sekarang", "sekitar", "sekitarnya", "sela", "selain", "selalu", "seluruh", "seluruhnya", "semakin", "sementara", "sempat",
		"semua", "semuanya", "sendiri", "sendirinya", "seolah", "seperti", "sepertinya", "sering", "seringnya", "serta", "siapa", "siapakah",
		"siapapun", "disini", "disinilah", "sini", "sinilah", "sesuatu", "sesuatunya", "suatu", "sesudah", "sesudahnya", "sudah", "sudahkah",
		"sudahlah", "supaya", "tadi", "tadinya", "tak", "tanpa", "setelah", "telah", "tentang", "tentu", "tentulah", "tentunya", "tertentu",
		"seterusnya", "tapi", "tetapi", "setiap", "tiap", "setidaknya", "tidak", "tidakkah", "tidaklah", "toh", "waduh", "wah", "wahai", "sewaktu",
		"walau", "walaupun", "wong", "yaitu", "yakni", "yang" 
	};
	
	public static final String[] STOP_WORDS_POLISH = {
		"aby", "ach", "acz", "aczkolwiek", "aj", "albo", "ale", "ale\u017C", "a\u017C", "bardziej", "bardzo", "bez", "bo", "bowiem", "byli",
		"bynajmniej", "by\u0107", "by\u0142", "by\u0142a", "by\u0142o", "by\u0142y", "b\u0119dzie", "b\u0119d\u0105", "cali", "ca\u0142a",
		"ca\u0142y", "ci", "ci\u0119", "ciebie", "co", "cokolwiek", "co\u015B", "czasami", "czasem", "czemu", "czy", "czyli", "daleko", "dla",
		"dlaczego", "dlatego", "dobrze", "dok\u0105d", "do\u015B\u0107", "du\u017Co", "dwa", "dwaj", "dwie", "dwoje", "dzi\u015B", "dzisiaj", "gdy",
		"gdyby", "gdy\u017C", "gdzie", "gdziekolwiek", "gdzie\u015B", "ich", "ile", "im", "inna", "inne", "inny", "innych", "i\u017C", "ja",
		"j\u0105", "jak", "jaka\u015B", "jakby", "jaki", "jakich\u015B", "jakie", "jaki\u015B", "jaki\u017C", "jakkolwiek", "jako", "jako\u015B",
		"je", "jeden", "jedna", "jedno", "jednak", "jednak\u017Ce", "jego", "jej", "jemu", "jest", "jestem", "jeszcze", "je\u015Bli", "je\u017Celi",
		"ju\u017C", "j\u0105", "ka\u017Cdy", "kiedy", "kilka", "kim\u015B", "kto", "ktokolwiek", "kto\u015B", "kt\u00F3ra", "kt\u00F3re",
		"kt\u00F3rego", "kt\u00F3rej", "kt\u00F3ry", "kt\u00F3rych", "kt\u00F3rym", "kt\u00F3rzy", "ku", "lat", "lecz", "lub", "ma", "maj\u0105",
		"mi", "mimo", "mi\u0119dzy", "mn\u0105", "mnie", "mog\u0105", "moi", "moim", "moja", "moje", "mo\u017Ce", "mo\u017Cliwe", "mo\u017Cna",
		"m\u00F3j", "mu", "musi", "na", "nad", "nam", "nami", "nas", "nasi", "nasz", "nasza", "nasze", "naszego", "naszych", "natomiast",
		"natychmiast", "nawet", "ni\u0105", "nic", "nich", "nie", "niego", "niej", "niemu", "nigdy", "nim", "nimi", "ni\u017C", "o", "obok", "od",
		"oko\u0142o", "ona", "oni", "ono", "oraz", "owszem", "pana", "pani", "po", "podczas", "pomimo", "poniewa\u017C", "powinien", "powinna",
		"powinni", "powinno", "poza", "prawie", "przecie\u017C", "przed", "przede", "przedtem", "przez", "przy", "roku", "r\u00F3wnie\u017C", "sam",
		"sama", "s\u0105", "si\u0119", "sk\u0105d", "sobie", "sob\u0105", "spos\u00F3b", "swoje", "s\u0105", "ta", "tak", "taka", "taki", "takie",
		"tak\u017Ce", "tam", "te", "tego", "tej", "teraz", "te\u017C", "tob\u0105", "tobie", "tote\u017C", "trzeba", "tu", "tutaj", "twoi", "twoim",
		"twoja", "twoje", "twym", "tw\u00F3j", "ty", "tych", "tylko", "tym", "u", "w", "wam", "wami", "wasz", "wasza", "wasze", "wed\u0142ug",
		"wiele", "wielu", "wi\u0119c", "wi\u0119cej", "wszyscy", "wszystkich", "wszystkie", "wszystkim", "wszystko", "wtedy", "wy",
		"w\u0142a\u015Bnie", "z", "za", "zapewne", "zawsze", "ze", "znowu", "zn\u00F3w", "zosta\u0142", "\u017Caden", "\u017Cadna", "\u017Cadne",
		"\u017Cadnych", "\u017Ce", "\u017Ceby" 
	};
	
	public static final String[] STOP_WORDS_SWEDISH = {
		"och", "det", "en", "jag", "hon", "som", "p\u00E5", "med", "var", "sig", "f\u00F6r", "s\u00E5", "\u00E4r", "ett", "om", "de", "av", "icke",
		"mig", "du", "henne", "d\u00E5", "nu", "har", "inte", "hans", "honom", "skulle", "hennes", "d\u00E4r", "ej", "vid", "kunde", "n\u00E5got",
		"fr\u00E5n", "ut", "n\u00E4r", "efter", "upp", "vi", "dem", "vara", "vad", "\u00F6ver", "\u00E4n", "h\u00E4r", "ha", "mot", "alla",
		"n\u00E5gon", "eller", "allt", "mycket", "ju", "denna", "sj\u00E4lv", "detta", "\u00E5t", "utan", "varit", "ingen", "ni", "bli", "blev",
		"oss", "dessa", "n\u00E5gra", "deras", "blir", "mina", "samma", "vilken", "er", "s\u00E5dan", "v\u00E5r", "blivit", "dess", "inom",
		"mellan", "s\u00E5dant", "varf\u00F6r", "varje", "vilka", "ditt", "vem", "vilket", "s\u00E5dana", "vart", "dina", "vars", "v\u00E5rt",
		"v\u00E5ra", "ert", "vilkas"
	};

	public static final String[] STOP_WORDS_KOREAN = {
		"\uB098\uB294", "\uB098\uB97C", "\uB098\uC758", "\uB0B4", "\uC790\uC2E0", "\uC6B0\uB9AC", "\uC6B0\uB9AC\uC758", "\uAC83",
		"\uC2A4\uC2A4\uB85C", "\uB2F9\uC2E0", "\uB2F9\uC2E0\uC758", "\uADF8", "\uADF8\uB97C", "\uADF8\uC758", "\uADF8\uB140", "\uADF8\uB140\uC758",
		"\uADF8\uAC83", "\uADF8\uAC83\uC758", "\uC790\uCCB4", "\uC0AC\uB78C\uB4E4", "\uADF8\uB4E4", "\uADF8\uB4E4\uC758", "\uBB34\uC5C7",
		"\uC5B4\uB290", "\uB204\uAD6C", "\uC774", "\uC774\uB4E4", "\uC624\uC804", "\uC774\uB2E4", "\uC544\uB974", "\uD588\uB2E4", "\uC788\uB2E4",
		"\uB3C4\uC6C0", "\uAC00", "\uD544\uC694", "\uD560", "\uD558\uC9C0", "\uD558\uAE30", "\uACFC", "\uD558\uC9C0\uB9CC", "\uBA74", "\uB610\uB294",
		"\uB54C\uBB38\uC5D0", "\uC73C\uB85C", "\uAE4C\uC9C0", "\uB3D9\uC548", "\uC5D0", "\uB85C", "\uC6A9", "\uC640", "\uC57D", "\uB300\uD558\uC5EC",
		"\uC0AC\uC774\uC5D0", "\uB97C", "\uD1B5\uD574", "\uC2DC", "\uC804\uC5D0", "\uD6C4", "\uC774\uC0C1", "\uC774\uD558", "\uBD80\uD130",
		"\uC62C\uB77C", "\uC544\uB798\uB85C", "\uBC16\uC73C\uB85C", "\uB5A8\uC5B4\uC838\uC11C", "\uC704\uC5D0", "\uC544\uB798\uC758", "\uB2E4\uC2DC",
		"\uB354", "\uBA40\uB9AC", "\uADF8\uB54C", "\uD55C", "\uBC88", "\uC5EC\uAE30\uC5D0", "\uADF8\uACF3\uC5D0", "\uC5B8\uC81C",
		"\uC5B4\uB514\uC5D0\uC11C", "\uC774\uC720", "\uBC29\uBC95", "\uBAA8\uB4E0", "\uC5B4\uB5A4", "\uBAA8\uB450", "\uAC01\uAC01\uC758",
		"\uC870\uAE08", "\uB354", "\uAC00\uC7A5", "\uB2E4\uB978", "\uB2E4\uC18C", "\uC774\uB7EC\uD55C", "\uC544\uB2C8", "\uC544\uB2C8", "\uB9CC",
		"\uC790\uC2E0\uC758", "\uAC19\uC740", "\uADF8\uB798\uC11C", "\uBCF4\uB2E4", "\uB108\uBB34", "\uB300\uB2E8\uD788", "\uC758", "\uD2F0",
		"\uC218", "\uC758\uC9C0", "\uB2E4\uB9CC", "\uB2D8", "\uB9CC\uC77C", "\uC9C0\uAE08"
	};

	public static final String[] STOP_WORDS_DUTCH = {
		"de", "en", "ik", "te", "dat", "een", "hij", "niet", "zijn", "op", "aan", "als", "voor", "er", "maar", "om", "zou", "mijn", "zo", "ze",
		"zich", "bij", "ook", "je", "mij", "der", "daar", "haar", "naar", "heb", "heeft", "hebben", "deze", "u", "nog", "zal", "zij", "nu", "ge",
		"geen", "omdat", "iets", "toch", "al", "veel", "meer", "doen", "moet", "zonder", "dus", "alles", "onder", "ja", "eens", "hier", "wie",
		"werd", "altijd", "doch", "wordt", "wezen", "kunnen", "ons", "zelf", "tegen", "na", "wil", "kon", "niets", "uw", "iemand", "geweest",
		"andere" 
	};

	public static final String[] STOP_WORDS_GERMAN = {
		"alle", "allem", "allen", "alles", "als", "ander", "andere", "anderem", "anderen", "anderer", "anderes", "anderm", "andern", "anderr",
		"anders", "auch", "auf", "aus", "bei", "bis", "bist", "da", "damit", "dann", "der", "des", "dem", "das", "da\u00DF", "derselbe", "derselben",
		"denselben", "desselben", "demselben", "dieselbe", "dieselben", "dasselbe", "dazu", "dein", "deine", "deinem", "deinen", "deiner", "deines",
		"denn", "derer", "dessen", "dich", "dir", "du", "dies", "diese", "diesem", "diesen", "dieser", "dieses", "doch", "dort", "durch", "ein",
		"eine", "einem", "einen", "einer", "eines", "einig", "einige", "einigem", "einigen", "einiger", "einiges", "einmal", "er", "ihn", "ihm",
		"es", "etwas", "euer", "eure", "eurem", "euren", "eurer", "eures", "f\u00FCr", "gegen", "gewesen", "hab", "habe", "haben", "hatte", "hatten",
		"hier", "hin", "hinter", "ich", "mich", "mir", "ihr", "ihre", "ihrem", "ihren", "ihrer", "ihres", "euch", "im", "indem", "ins", "ist",
		"jede", "jedem", "jeden", "jeder", "jedes", "jene", "jenem", "jenen", "jener", "jenes", "jetzt", "kann", "kein", "keine", "keinem", "keinen",
		"keiner", "keines", "k\u00F6nnen", "k\u00F6nnte", "machen", "man", "manche", "manchem", "manchen", "mancher", "manches", "mein", "meine",
		"meinem", "meinen", "meiner", "meines", "mit", "muss", "musste", "nach", "nicht", "nichts", "noch", "nur", "ob", "oder", "ohne", "sehr",
		"sein", "seine", "seinem", "seinen", "seiner", "seines", "selbst", "sich", "sie", "ihnen", "sind", "solche", "solchem", "solchen", "solcher",
		"solches", "soll", "sollte", "sondern", "sonst", "\u00FCber", "um", "und", "uns", "unse", "unsem", "unsen", "unser", "unses", "unter",
		"viel", "vom", "von", "vor", "w\u00E4hrend", "waren", "warst", "weg", "weil", "weiter", "welche", "welchem", "welchen", "welcher", "welches",
		"wenn", "werde", "werden", "wie", "wieder", "will", "wir", "wird", "wirst", "wo", "wollen", "wollte", "w\u00FCrde", "w\u00FCrden", "zu",
		"zum", "zur", "zwar", "zwischen" 
	};

	public static final String[] STOP_WORDS_GREEK = {
		"\u03BF", "\u03B7", "\u03C4\u03BF", "\u03BF\u03B9", "\u03C4\u03B1", "\u03C4\u03BF\u03C5", "\u03C4\u03B7\u03C3", "\u03C4\u03C9\u03BD",
		"\u03C4\u03BF\u03BD", "\u03C4\u03B7\u03BD", "\u03BA\u03B1\u03B9", "\u03BA\u03B9", "\u03BA", "\u03B5\u03B9\u03BC\u03B1\u03B9",
		"\u03B5\u03B9\u03C3\u03B1\u03B9", "\u03B5\u03B9\u03BD\u03B1\u03B9", "\u03B5\u03B9\u03BC\u03B1\u03C3\u03C4\u03B5",
		"\u03B5\u03B9\u03C3\u03C4\u03B5", "\u03C3\u03C4\u03BF", "\u03C3\u03C4\u03BF\u03BD", "\u03C3\u03C4\u03B7", "\u03C3\u03C4\u03B7\u03BD",
		"\u03BC\u03B1", "\u03B1\u03BB\u03BB\u03B1", "\u03B1\u03C0\u03BF", "\u03B3\u03B9\u03B1", "\u03C0\u03C1\u03BF\u03C3", "\u03BC\u03B5",
		"\u03C3\u03B5", "\u03C9\u03C3", "\u03C0\u03B1\u03C1\u03B1", "\u03B1\u03BD\u03C4\u03B9", "\u03BA\u03B1\u03C4\u03B1",
		"\u03BC\u03B5\u03C4\u03B1", "\u03B8\u03B1", "\u03BD\u03B1", "\u03B4\u03B5", "\u03B4\u03B5\u03BD", "\u03BC\u03B7", "\u03BC\u03B7\u03BD",
		"\u03B5\u03C0\u03B9", "\u03B5\u03BD\u03C9", "\u03B5\u03B1\u03BD", "\u03B1\u03BD", "\u03C4\u03BF\u03C4\u03B5", "\u03C0\u03BF\u03C5",
		"\u03C0\u03C9\u03C3", "\u03C0\u03BF\u03B9\u03BF\u03C3", "\u03C0\u03BF\u03B9\u03B1", "\u03C0\u03BF\u03B9\u03BF",
		"\u03C0\u03BF\u03B9\u03BF\u03B9", "\u03C0\u03BF\u03B9\u03B5\u03C3", "\u03C0\u03BF\u03B9\u03C9\u03BD", "\u03C0\u03BF\u03B9\u03BF\u03C5\u03C3",
		"\u03B1\u03C5\u03C4\u03BF\u03C3", "\u03B1\u03C5\u03C4\u03B7", "\u03B1\u03C5\u03C4\u03BF", "\u03B1\u03C5\u03C4\u03BF\u03B9",
		"\u03B1\u03C5\u03C4\u03C9\u03BD", "\u03B1\u03C5\u03C4\u03BF\u03C5\u03C3", "\u03B1\u03C5\u03C4\u03B5\u03C3", "\u03B1\u03C5\u03C4\u03B1",
		"\u03B5\u03BA\u03B5\u03B9\u03BD\u03BF\u03C3", "\u03B5\u03BA\u03B5\u03B9\u03BD\u03B7", "\u03B5\u03BA\u03B5\u03B9\u03BD\u03BF",
		"\u03B5\u03BA\u03B5\u03B9\u03BD\u03BF\u03B9", "\u03B5\u03BA\u03B5\u03B9\u03BD\u03B5\u03C3", "\u03B5\u03BA\u03B5\u03B9\u03BD\u03B1",
		"\u03B5\u03BA\u03B5\u03B9\u03BD\u03C9\u03BD", "\u03B5\u03BA\u03B5\u03B9\u03BD\u03BF\u03C5\u03C3", "\u03BF\u03C0\u03C9\u03C3",
		"\u03BF\u03BC\u03C9\u03C3", "\u03B9\u03C3\u03C9\u03C3", "\u03BF\u03C3\u03BF", "\u03BF\u03C4\u03B9"
	};

	public static final String[] STOP_WORDS_RUSSIAN = {
		"\u0438", "\u0432", "\u0432\u043E", "\u043D\u0435", "\u0447\u0442\u043E", "\u043E\u043D", "\u043D\u0430", "\u044F", "\u0441", "\u0441\u043E",
		"\u043A\u0430\u043A", "\u0430", "\u0442\u043E", "\u0432\u0441\u0435", "\u043E\u043D\u0430", "\u0442\u0430\u043A", "\u0435\u0433\u043E",
		"\u043D\u043E", "\u0434\u0430", "\u0442\u044B", "\u043A", "\u0443", "\u0436\u0435", "\u0432\u044B", "\u0437\u0430", "\u0431\u044B",
		"\u043F\u043E", "\u0442\u043E\u043B\u044C\u043A\u043E", "\u0435\u0435", "\u043C\u043D\u0435", "\u0431\u044B\u043B\u043E",
		"\u0432\u043E\u0442", "\u043E\u0442", "\u043C\u0435\u043D\u044F", "\u0435\u0449\u0435", "\u043D\u0435\u0442", "\u043E", "\u0438\u0437",
		"\u0435\u043C\u0443", "\u0442\u0435\u043F\u0435\u0440\u044C", "\u043A\u043E\u0433\u0434\u0430", "\u0434\u0430\u0436\u0435", "\u043D\u0443",
		"\u0432\u0434\u0440\u0443\u0433", "\u043B\u0438", "\u0435\u0441\u043B\u0438", "\u0443\u0436\u0435", "\u0438\u043B\u0438", "\u043D\u0438",
		"\u0431\u044B\u0442\u044C", "\u0431\u044B\u043B", "\u043D\u0435\u0433\u043E", "\u0434\u043E", "\u0432\u0430\u0441",
		"\u043D\u0438\u0431\u0443\u0434\u044C", "\u043E\u043F\u044F\u0442\u044C", "\u0443\u0436", "\u0432\u0430\u043C", "\u0432\u0435\u0434\u044C",
		"\u0442\u0430\u043C", "\u043F\u043E\u0442\u043E\u043C", "\u0441\u0435\u0431\u044F", "\u043D\u0438\u0447\u0435\u0433\u043E", "\u0435\u0439",
		"\u043C\u043E\u0436\u0435\u0442", "\u043E\u043D\u0438", "\u0442\u0443\u0442", "\u0433\u0434\u0435", "\u0435\u0441\u0442\u044C",
		"\u043D\u0430\u0434\u043E", "\u043D\u0435\u0439", "\u0434\u043B\u044F", "\u043C\u044B", "\u0442\u0435\u0431\u044F", "\u0438\u0445",
		"\u0447\u0435\u043C", "\u0431\u044B\u043B\u0430", "\u0441\u0430\u043C", "\u0447\u0442\u043E\u0431", "\u0431\u0435\u0437",
		"\u0431\u0443\u0434\u0442\u043E", "\u0447\u0435\u0433\u043E", "\u0440\u0430\u0437", "\u0442\u043E\u0436\u0435", "\u0441\u0435\u0431\u0435",
		"\u043F\u043E\u0434", "\u0431\u0443\u0434\u0435\u0442", "\u0436", "\u0442\u043E\u0433\u0434\u0430", "\u043A\u0442\u043E",
		"\u044D\u0442\u043E\u0442", "\u0442\u043E\u0433\u043E", "\u043F\u043E\u0442\u043E\u043C\u0443", "\u044D\u0442\u043E\u0433\u043E",
		"\u043A\u0430\u043A\u043E\u0439", "\u0441\u043E\u0432\u0441\u0435\u043C", "\u043D\u0438\u043C", "\u0437\u0434\u0435\u0441\u044C",
		"\u044D\u0442\u043E\u043C", "\u043E\u0434\u0438\u043D", "\u043F\u043E\u0447\u0442\u0438", "\u043C\u043E\u0439", "\u0442\u0435\u043C",
		"\u0447\u0442\u043E\u0431\u044B", "\u043D\u0435\u0435", "\u0441\u0435\u0439\u0447\u0430\u0441", "\u0431\u044B\u043B\u0438",
		"\u043A\u0443\u0434\u0430", "\u0437\u0430\u0447\u0435\u043C", "\u0432\u0441\u0435\u0445", "\u043D\u0438\u043A\u043E\u0433\u0434\u0430",
		"\u043C\u043E\u0436\u043D\u043E", "\u043F\u0440\u0438", "\u043D\u0430\u043A\u043E\u043D\u0435\u0446", "\u0434\u0432\u0430", "\u043E\u0431",
		"\u0434\u0440\u0443\u0433\u043E\u0439", "\u0445\u043E\u0442\u044C", "\u043F\u043E\u0441\u043B\u0435", "\u043D\u0430\u0434",
		"\u0431\u043E\u043B\u044C\u0448\u0435", "\u0442\u043E\u0442", "\u0447\u0435\u0440\u0435\u0437", "\u044D\u0442\u0438", "\u043D\u0430\u0441",
		"\u043F\u0440\u043E", "\u0432\u0441\u0435\u0433\u043E", "\u043D\u0438\u0445", "\u043A\u0430\u043A\u0430\u044F",
		"\u043C\u043D\u043E\u0433\u043E", "\u0440\u0430\u0437\u0432\u0435", "\u0442\u0440\u0438", "\u044D\u0442\u0443", "\u043C\u043E\u044F",
		"\u0432\u043F\u0440\u043E\u0447\u0435\u043C", "\u0445\u043E\u0440\u043E\u0448\u043E", "\u0441\u0432\u043E\u044E", "\u044D\u0442\u043E\u0439",
		"\u043F\u0435\u0440\u0435\u0434", "\u0438\u043D\u043E\u0433\u0434\u0430", "\u043B\u0443\u0447\u0448\u0435", "\u0447\u0443\u0442\u044C",
		"\u0442\u043E\u043C", "\u043D\u0435\u043B\u044C\u0437\u044F", "\u0442\u0430\u043A\u043E\u0439", "\u0438\u043C",
		"\u0431\u043E\u043B\u0435\u0435", "\u0432\u0441\u0435\u0433\u0434\u0430", "\u043A\u043E\u043D\u0435\u0447\u043D\u043E", "\u0432\u0441\u044E",
		"\u043C\u0435\u0436\u0434\u0443"
	};

	public static final String[] STOP_WORDS_PERSIAN = {
		"\u0648", "\u062F\u0631", "\u0628\u0647", "\u0627\u0632", "\u0643\u0647", "\u0645\u064A", "\u0627\u064A\u0646", "\u0627\u0633\u062A",
		"\u0631\u0627", "\u0628\u0627", "\u0647\u0627\u064A", "\u0628\u0631\u0627\u064A", "\u0622\u0646", "\u064A\u0643", "\u0634\u0648\u062F",
		"\u0634\u062F\u0647", "\u062E\u0648\u062F", "\u0647\u0627", "\u0643\u0631\u062F", "\u0634\u062F", "\u0627\u064A", "\u062A\u0627",
		"\u0643\u0646\u062F", "\u0628\u0631", "\u0628\u0648\u062F", "\u06AF\u0641\u062A", "\u0646\u064A\u0632", "\u0648\u064A", "\u0647\u0645",
		"\u0643\u0646\u0646\u062F", "\u062F\u0627\u0631\u062F", "\u0645\u0627", "\u0643\u0631\u062F\u0647", "\u064A\u0627", "\u0627\u0645\u0627",
		"\u0628\u0627\u064A\u062F", "\u062F\u0648", "\u0627\u0646\u062F", "\u0647\u0631", "\u062E\u0648\u0627\u0647\u062F", "\u0627\u0648",
		"\u0645\u0648\u0631\u062F", "\u0622\u0646\u0647\u0627", "\u0628\u0627\u0634\u062F", "\u062F\u064A\u06AF\u0631", "\u0645\u0631\u062F\u0645",
		"\u0646\u0645\u064A", "\u0628\u064A\u0646", "\u067E\u064A\u0634", "\u067E\u0633", "\u0627\u06AF\u0631", "\u0647\u0645\u0647",
		"\u0635\u0648\u0631\u062A", "\u064A\u0643\u064A", "\u0647\u0633\u062A\u0646\u062F", "\u0628\u064A", "\u0645\u0646", "\u062F\u0647\u062F",
		"\u0647\u0632\u0627\u0631", "\u0646\u064A\u0633\u062A", "\u0627\u0633\u062A\u0641\u0627\u062F\u0647", "\u062F\u0627\u062F",
		"\u062F\u0627\u0634\u062A\u0647", "\u0631\u0627\u0647", "\u062F\u0627\u0634\u062A", "\u0686\u0647", "\u0647\u0645\u0686\u0646\u064A\u0646",
		"\u0643\u0631\u062F\u0646\u062F", "\u062F\u0627\u062F\u0647", "\u0628\u0648\u062F\u0647", "\u062F\u0627\u0631\u0646\u062F",
		"\u0647\u0645\u064A\u0646", "\u0645\u064A\u0644\u064A\u0648\u0646", "\u0633\u0648\u064A", "\u0634\u0648\u0646\u062F",
		"\u0628\u064A\u0634\u062A\u0631", "\u0628\u0633\u064A\u0627\u0631", "\u0631\u0648\u064A", "\u06AF\u0631\u0641\u062A\u0647",
		"\u0647\u0627\u064A\u064A", "\u062A\u0648\u0627\u0646\u062F", "\u0627\u0648\u0644", "\u0646\u0627\u0645", "\u0647\u064A\u0686",
		"\u0686\u0646\u062F", "\u062C\u062F\u064A\u062F", "\u0628\u064A\u0634", "\u0634\u062F\u0646", "\u0643\u0631\u062F\u0646",
		"\u0643\u0646\u064A\u0645", "\u0646\u0634\u0627\u0646", "\u062D\u062A\u064A", "\u0627\u064A\u0646\u0643\u0647", "\u0648\u0644\u06CC",
		"\u062A\u0648\u0633\u0637", "\u0686\u0646\u064A\u0646", "\u0628\u0631\u062E\u064A", "\u0646\u0647", "\u062F\u064A\u0631\u0648\u0632",
		"\u062F\u0648\u0645", "\u062F\u0631\u0628\u0627\u0631\u0647", "\u0628\u0639\u062F", "\u0645\u062E\u062A\u0644\u0641",
		"\u06AF\u064A\u0631\u062F", "\u0634\u0645\u0627", "\u06AF\u0641\u062A\u0647", "\u0622\u0646\u0627\u0646", "\u0628\u0627\u0631",
		"\u0637\u0648\u0631", "\u06AF\u0631\u0641\u062A", "\u062F\u0647\u0646\u062F", "\u06AF\u0630\u0627\u0631\u064A",
		"\u0628\u0633\u064A\u0627\u0631\u064A", "\u0637\u064A", "\u0628\u0648\u062F\u0646\u062F", "\u0645\u064A\u0644\u064A\u0627\u0631\u062F",
		"\u0628\u062F\u0648\u0646", "\u062A\u0645\u0627\u0645", "\u0643\u0644", "\u062A\u0631", "\u0628\u0631\u0627\u0633\u0627\u0633",
		"\u0634\u062F\u0646\u062F", "\u062A\u0631\u064A\u0646", "\u0627\u0645\u0631\u0648\u0632", "\u0628\u0627\u0634\u0646\u062F",
		"\u0646\u062F\u0627\u0631\u062F", "\u0686\u0648\u0646", "\u0642\u0627\u0628\u0644", "\u06AF\u0648\u064A\u062F",
		"\u062F\u064A\u06AF\u0631\u064A", "\u0647\u0645\u0627\u0646", "\u062E\u0648\u0627\u0647\u0646\u062F", "\u0642\u0628\u0644",
		"\u0622\u0645\u062F\u0647", "\u0627\u0643\u0646\u0648\u0646", "\u062A\u062D\u062A", "\u0637\u0631\u064A\u0642", "\u06AF\u064A\u0631\u064A",
		"\u062C\u0627\u064A", "\u0647\u0646\u0648\u0632", "\u0686\u0631\u0627", "\u0627\u0644\u0628\u062A\u0647", "\u0643\u0646\u064A\u062F",
		"\u0633\u0627\u0632\u064A", "\u0633\u0648\u0645", "\u0643\u0646\u0645", "\u0628\u0644\u0643\u0647", "\u0632\u064A\u0631",
		"\u062A\u0648\u0627\u0646\u0646\u062F", "\u0636\u0645\u0646", "\u0641\u0642\u0637", "\u0628\u0648\u062F\u0646", "\u062D\u0642",
		"\u0622\u064A\u062F", "\u0648\u0642\u062A\u064A", "\u0627\u0634", "\u064A\u0627\u0628\u062F", "\u0646\u062E\u0633\u062A\u064A\u0646",
		"\u0645\u0642\u0627\u0628\u0644", "\u062E\u062F\u0645\u0627\u062A", "\u0627\u0645\u0633\u0627\u0644", "\u062A\u0627\u0643\u0646\u0648\u0646",
		"\u0645\u0627\u0646\u0646\u062F", "\u062A\u0627\u0632\u0647", "\u0622\u0648\u0631\u062F", "\u0641\u0643\u0631", "\u0622\u0646\u0686\u0647",
		"\u0646\u062E\u0633\u062A", "\u0646\u0634\u062F\u0647", "\u0634\u0627\u064A\u062F", "\u0686\u0647\u0627\u0631",
		"\u062C\u0631\u064A\u0627\u0646", "\u067E\u0646\u062C", "\u0633\u0627\u062E\u062A\u0647", "\u0632\u064A\u0631\u0627",
		"\u0646\u0632\u062F\u064A\u0643", "\u0628\u0631\u062F\u0627\u0631\u064A", "\u0643\u0633\u064A", "\u0631\u064A\u0632\u064A",
		"\u0631\u0641\u062A", "\u06AF\u0631\u062F\u062F", "\u0645\u062B\u0644", "\u0622\u0645\u062F", "\u0627\u0645",
		"\u0628\u0647\u062A\u0631\u064A\u0646", "\u062F\u0627\u0646\u0633\u062A", "\u0643\u0645\u062A\u0631", "\u062F\u0627\u062F\u0646",
		"\u062A\u0645\u0627\u0645\u064A", "\u062C\u0644\u0648\u06AF\u064A\u0631\u064A", "\u0628\u064A\u0634\u062A\u0631\u064A", "\u0627\u064A\u0645",
		"\u0646\u0627\u0634\u064A", "\u0686\u064A\u0632\u064A", "\u0622\u0646\u0643\u0647", "\u0628\u0627\u0644\u0627",
		"\u0628\u0646\u0627\u0628\u0631\u0627\u064A\u0646", "\u0627\u064A\u0634\u0627\u0646", "\u0628\u0639\u0636\u064A",
		"\u062F\u0627\u062F\u0646\u062F", "\u062F\u0627\u0634\u062A\u0646\u062F", "\u0628\u0631\u062E\u0648\u0631\u062F\u0627\u0631",
		"\u0646\u062E\u0648\u0627\u0647\u062F", "\u0647\u0646\u06AF\u0627\u0645", "\u0646\u0628\u0627\u064A\u062F", "\u063A\u064A\u0631",
		"\u0646\u0628\u0648\u062F", "\u062F\u064A\u062F\u0647", "\u0648\u06AF\u0648", "\u062F\u0627\u0631\u064A\u0645",
		"\u0686\u06AF\u0648\u0646\u0647", "\u0628\u0646\u062F\u064A", "\u062E\u0648\u0627\u0633\u062A", "\u0641\u0648\u0642", "\u062F\u0647",
		"\u0646\u0648\u0639\u064A", "\u0647\u0633\u062A\u064A\u0645", "\u062F\u064A\u06AF\u0631\u0627\u0646", "\u0647\u0645\u0686\u0646\u0627\u0646",
		"\u0633\u0631\u0627\u0633\u0631", "\u0646\u062F\u0627\u0631\u0646\u062F", "\u06AF\u0631\u0648\u0647\u064A", "\u0633\u0639\u064A",
		"\u0631\u0648\u0632\u0647\u0627\u064A", "\u0622\u0646\u062C\u0627", "\u064A\u0643\u062F\u064A\u06AF\u0631", "\u0643\u0631\u062F\u0645",
		"\u0628\u064A\u0633\u062A", "\u0628\u0631\u0648\u0632", "\u0633\u067E\u0633", "\u0631\u0641\u062A\u0647", "\u0622\u0648\u0631\u062F\u0647",
		"\u0646\u0645\u0627\u064A\u062F", "\u0628\u0627\u0634\u064A\u0645", "\u06AF\u0648\u064A\u0646\u062F", "\u0632\u064A\u0627\u062F",
		"\u062E\u0648\u064A\u0634", "\u0647\u0645\u0648\u0627\u0631\u0647", "\u06AF\u0630\u0627\u0634\u062A\u0647", "\u0634\u0634",
		"\u0646\u062F\u0627\u0634\u062A\u0647", "\u0634\u0646\u0627\u0633\u064A", "\u062E\u0648\u0627\u0647\u064A\u0645", "\u0622\u0628\u0627\u062F",
		"\u062F\u0627\u0634\u062A\u0646", "\u0646\u0638\u064A\u0631", "\u0647\u0645\u0686\u0648\u0646", "\u0628\u0627\u0631\u0647",
		"\u0646\u0643\u0631\u062F\u0647", "\u0634\u0627\u0646", "\u0633\u0627\u0628\u0642", "\u0647\u0641\u062A", "\u062F\u0627\u0646\u0646\u062F",
		"\u062C\u0627\u064A\u064A", "\u0628\u06CC", "\u062C\u0632", "\u0632\u06CC\u0631\u0650", "\u0631\u0648\u06CC\u0650",
		"\u0633\u0631\u06CC\u0650", "\u062A\u0648\u06CC\u0650", "\u062C\u0644\u0648\u06CC\u0650", "\u067E\u06CC\u0634\u0650",
		"\u0639\u0642\u0628\u0650", "\u0628\u0627\u0644\u0627\u06CC\u0650", "\u062E\u0627\u0631\u062C\u0650", "\u0648\u0633\u0637\u0650",
		"\u0628\u06CC\u0631\u0648\u0646\u0650", "\u0633\u0648\u06CC\u0650", "\u06A9\u0646\u0627\u0631\u0650", "\u067E\u0627\u0639\u06CC\u0646\u0650",
		"\u0646\u0632\u062F\u0650", "\u0646\u0632\u062F\u06CC\u06A9\u0650", "\u062F\u0646\u0628\u0627\u0644\u0650", "\u062D\u062F\u0648\u062F\u0650",
		"\u0628\u0631\u0627\u0628\u0631\u0650", "\u0637\u0628\u0642\u0650", "\u0645\u0627\u0646\u0646\u062F\u0650", "\u0636\u062F\u0651\u0650",
		"\u0647\u0646\u06AF\u0627\u0645\u0650", "\u0628\u0631\u0627\u06CC\u0650", "\u0645\u062B\u0644\u0650", "\u0628\u0627\u0631\u0629",
		"\u0627\u062B\u0631\u0650", "\u062A\u0648\u0644\u0650", "\u0639\u0644\u0651\u062A\u0650", "\u0633\u0645\u062A\u0650",
		"\u0639\u0646\u0648\u0627\u0646\u0650", "\u0642\u0635\u062F\u0650", "\u0631\u0648\u0628", "\u062C\u062F\u0627", "\u06A9\u06CC",
		"\u06A9\u0647", "\u0686\u06CC\u0633\u062A", "\u0647\u0633\u062A", "\u06A9\u062C\u0627", "\u06A9\u062C\u0627\u0633\u062A",
		"\u06A9\u064E\u06CC", "\u0686\u0637\u0648\u0631", "\u06A9\u062F\u0627\u0645", "\u0622\u06CC\u0627", "\u0645\u06AF\u0631",
		"\u0686\u0646\u062F\u06CC\u0646", "\u06CC\u06A9", "\u0686\u06CC\u0632\u06CC", "\u062F\u06CC\u06AF\u0631", "\u06A9\u0633\u06CC",
		"\u0628\u0639\u0631\u06CC", "\u0647\u06CC\u0686", "\u0686\u06CC\u0632", "\u062C\u0627", "\u06A9\u0633", "\u0647\u0631\u06AF\u0632",
		"\u06CC\u0627", "\u062A\u0646\u0647\u0627", "\u0628\u0644\u06A9\u0647", "\u062E\u06CC\u0627\u0647", "\u0628\u0644\u0647",
		"\u0628\u0644\u06CC", "\u0622\u0631\u0647", "\u0622\u0631\u06CC", "\u0645\u0631\u0633\u06CC", "\u0627\u0644\u0628\u062A\u0651\u0647",
		"\u0644\u0637\u0641\u0627\u064B", "\u0651\u0647", "\u0627\u0646\u06A9\u0647", "\u0648\u0642\u062A\u06CC\u06A9\u0647",
		"\u0647\u0645\u06CC\u0646", "\u067E\u06CC\u0634", "\u0645\u062F\u0651\u062A\u06CC", "\u0647\u0646\u06AF\u0627\u0645\u06CC",
		"\u0645\u0627\u0646", "\u062A\u0627\u0646" 
	};

	public static final String[] STOP_WORDS_SPANISH = {
		"de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las", "por", "un", "para", "con", "una", "su", "al", "lo", "como", "m\u00E1s",
		"pero", "sus", "le", "ya", "o", "este", "s\u00ED", "porque", "esta", "entre", "cuando", "muy", "sobre", "tambi\u00E9n", "hasta", "donde",
		"quien", "desde", "todo", "nos", "durante", "todos", "uno", "les", "ni", "contra", "otros", "ese", "eso", "ante", "ellos", "e", "esto",
		"m\u00ED", "antes", "algunos", "qu\u00E9", "unos", "yo", "otro", "otras", "otra", "\u00E9l", "tanto", "esa", "estos", "mucho", "quienes",
		"nada", "muchos", "cual", "poco", "ella", "estar", "estas", "algunas", "algo", "nosotros", "mi", "mis", "t\u00FA", "te", "ti", "tu", "tus",
		"ellas", "nosotras", "vosostros", "vosostras", "m\u00EDo", "m\u00EDa", "m\u00EDos", "m\u00EDas", "tuyo", "tuya", "tuyos", "tuyas", "suyo",
		"suya", "suyos", "suyas", "nuestro", "nuestra", "nuestros", "nuestras", "vuestro", "vuestra", "vuestros", "vuestras", "esos", "esas",
		"estoy", "est\u00E1s", "est\u00E1", "estamos", "est\u00E1is", "est\u00E1n", "est\u00E9", "est\u00E9s", "estemos", "est\u00E9is",
		"est\u00E9n", "estar\u00E9", "estar\u00E1s", "estar\u00E1", "estaremos", "estar\u00E9is", "estar\u00E1n", "estar\u00EDa", "estar\u00EDas",
		"estar\u00EDamos", "estar\u00EDais", "estar\u00EDan", "estaba", "estabas", "est\u00E1bamos", "estabais", "estaban", "estuve", "estuviste",
		"estuvo", "estuvimos", "estuvisteis", "estuvieron", "estuviera", "estuvieras", "estuvi\u00E9ramos", "estuvierais", "estuvieran", "estuviese",
		"estuvieses", "estuvi\u00E9semos", "estuvieseis", "estuviesen", "estando", "estado", "estada", "estados", "estadas", "estad", "ha", "hemos",
		"hab\u00E9is", "haya", "hayas", "hayamos", "hay\u00E1is", "hayan", "habr\u00E9", "habr\u00E1s", "habr\u00E1", "habremos", "habr\u00E9is",
		"habr\u00E1n", "habr\u00EDa", "habr\u00EDas", "habr\u00EDamos", "habr\u00EDais", "habr\u00EDan", "hab\u00EDa", "hab\u00EDas",
		"hab\u00EDamos", "hab\u00EDais", "hab\u00EDan", "hube", "hubiste", "hubo", "hubimos", "hubisteis", "hubieron", "hubiera", "hubieras",
		"hubi\u00E9ramos", "hubierais", "hubieran", "hubiese", "hubieses", "hubi\u00E9semos", "hubieseis", "hubiesen", "habiendo", "habido",
		"habida", "habidos", "habidas", "eres", "es", "somos", "sois", "seas", "seamos", "se\u00E1is", "sean", "ser\u00E9", "ser\u00E1s",
		"ser\u00E1", "seremos", "ser\u00E9is", "ser\u00E1n", "ser\u00EDa", "ser\u00EDas", "ser\u00EDamos", "ser\u00EDais", "ser\u00EDan", "eras",
		"\u00E9ramos", "erais", "eran", "fui", "fuiste", "fue", "fuimos", "fuisteis", "fueron", "fuera", "fueras", "fu\u00E9ramos", "fuerais",
		"fueran", "fuese", "fueses", "fu\u00E9semos", "fueseis", "fuesen", "sintiendo", "sentido", "sentida", "sentidos", "sentidas", "siente",
		"sentid", "tengo", "tienes", "tiene", "tenemos", "ten\u00E9is", "tienen", "tenga", "tengas", "tengamos", "teng\u00E1is", "tengan",
		"tendr\u00E9", "tendr\u00E1s", "tendr\u00E1", "tendremos", "tendr\u00E9is", "tendr\u00E1n", "tendr\u00EDa", "tendr\u00EDas",
		"tendr\u00EDamos", "tendr\u00EDais", "tendr\u00EDan", "ten\u00EDa", "ten\u00EDas", "ten\u00EDamos", "ten\u00EDais", "ten\u00EDan", "tuve",
		"tuviste", "tuvo", "tuvimos", "tuvisteis", "tuvieron", "tuviera", "tuvieras", "tuvi\u00E9ramos", "tuvierais", "tuvieran", "tuviese",
		"tuvieses", "tuvi\u00E9semos", "tuvieseis", "tuviesen", "teniendo", "tenido", "tenida", "tenidos", "tenidas", "tened" 
	};

	public static final String[] STOP_WORDS_HINDI = {
		"\u092A\u0930", "\u0907\u0928", "\u0935\u0939", "\u092F\u093F\u0939", "\u0935\u0941\u0939", "\u091C\u093F\u0928\u094D\u0939\u0947\u0902",
		"\u091C\u093F\u0928\u094D\u0939\u094B\u0902", "\u0924\u093F\u0928\u094D\u0939\u0947\u0902", "\u0924\u093F\u0928\u094D\u0939\u094B\u0902",
		"\u0915\u093F\u0928\u094D\u0939\u094B\u0902", "\u0915\u093F\u0928\u094D\u0939\u0947\u0902", "\u0907\u0924\u094D\u092F\u093E\u0926\u093F",
		"\u0926\u094D\u0935\u093E\u0930\u093E", "\u0907\u0928\u094D\u0939\u0947\u0902", "\u0907\u0928\u094D\u0939\u094B\u0902",
		"\u0909\u0928\u094D\u0939\u094B\u0902", "\u092C\u093F\u0932\u0915\u0941\u0932", "\u0928\u093F\u0939\u093E\u092F\u0924",
		"\u0931\u094D\u0935\u093E\u0938\u093E", "\u0907\u0928\u094D\u0939\u0940\u0902", "\u0909\u0928\u094D\u0939\u0940\u0902",
		"\u0909\u0928\u094D\u0939\u0947\u0902", "\u0907\u0938\u092E\u0947\u0902", "\u091C\u093F\u0924\u0928\u093E", "\u0926\u0941\u0938\u0930\u093E",
		"\u0915\u093F\u0924\u0928\u093E", "\u0926\u092C\u093E\u0930\u093E", "\u0938\u093E\u092C\u0941\u0924", "\u0935\u095A\u0948\u0930\u0939",
		"\u0926\u0942\u0938\u0930\u0947", "\u0915\u094C\u0928\u0938\u093E", "\u0932\u0947\u0915\u093F\u0928", "\u0939\u094B\u0924\u093E",
		"\u0915\u0930\u0928\u0947", "\u0915\u093F\u092F\u093E", "\u0932\u093F\u092F\u0947", "\u0905\u092A\u0928\u0947", "\u0928\u0939\u0940\u0902",
		"\u0926\u093F\u092F\u093E", "\u0907\u0938\u0915\u093E", "\u0915\u0930\u0928\u093E", "\u0935\u093E\u0932\u0947", "\u0938\u0915\u0924\u0947",
		"\u0907\u0938\u0915\u0947", "\u0938\u092C\u0938\u0947", "\u0939\u094B\u0928\u0947", "\u0915\u0930\u0924\u0947", "\u092C\u0939\u0941\u0924",
		"\u0935\u0930\u094D\u0917", "\u0915\u0930\u0947\u0902", "\u0939\u094B\u0924\u0940", "\u0905\u092A\u0928\u0940", "\u0909\u0928\u0915\u0947",
		"\u0915\u0939\u0924\u0947", "\u0939\u094B\u0924\u0947", "\u0915\u0930\u0924\u093E", "\u0909\u0928\u0915\u0940", "\u0907\u0938\u0915\u0940",
		"\u0938\u0915\u0924\u093E", "\u0930\u0916\u0947\u0902", "\u0905\u092A\u0928\u093E", "\u0909\u0938\u0915\u0947", "\u091C\u093F\u0938\u0947",
		"\u0924\u093F\u0938\u0947", "\u0915\u093F\u0938\u0947", "\u0915\u093F\u0938\u0940", "\u0915\u093E\u095E\u0940", "\u092A\u0939\u0932\u0947",
		"\u0928\u0940\u091A\u0947", "\u092C\u093E\u0932\u093E", "\u092F\u0939\u093E\u0901", "\u091C\u0948\u0938\u093E", "\u091C\u0948\u0938\u0947",
		"\u092E\u093E\u0928\u094B", "\u0905\u0902\u0926\u0930", "\u092D\u0940\u0924\u0930", "\u092A\u0942\u0930\u093E", "\u0938\u093E\u0930\u093E",
		"\u0939\u094B\u0928\u093E", "\u0909\u0928\u0915\u094B", "\u0935\u0939\u093E\u0901", "\u0935\u0939\u0940\u0902", "\u091C\u0939\u093E\u0901",
		"\u091C\u0940\u0927\u0930", "\u0909\u0928\u0915\u093E", "\u0907\u0928\u0915\u093E", "\uFEFF\u0915\u0947", "\u0939\u0948\u0902",
		"\u0917\u092F\u093E", "\u092C\u0928\u0940", "\u090F\u0935\u0902", "\u0939\u0941\u0906", "\u0938\u093E\u0925", "\u092C\u093E\u0926",
		"\u0932\u093F\u090F", "\u0915\u0941\u091B", "\u0915\u0939\u093E", "\u092F\u0926\u093F", "\u0939\u0941\u0908", "\u0907\u0938\u0947",
		"\u0939\u0941\u090F", "\u0905\u092D\u0940", "\u0938\u092D\u0940", "\u0915\u0941\u0932", "\u0930\u0939\u093E", "\u0930\u0939\u0947",
		"\u0907\u0938\u0940", "\u0909\u0938\u0947", "\u091C\u093F\u0938", "\u091C\u093F\u0928", "\u0924\u093F\u0938", "\u0924\u093F\u0928",
		"\u0915\u094C\u0928", "\u0915\u093F\u0938", "\u0915\u094B\u0908", "\u0910\u0938\u0947", "\u0924\u0930\u0939", "\u0915\u093F\u0930",
		"\u0938\u093E\u092D", "\u0938\u0902\u0917", "\u092F\u0939\u0940", "\u092C\u0939\u0940", "\u0909\u0938\u0940", "\u092B\u093F\u0930",
		"\u092E\u0917\u0930", "\u0915\u093E", "\u090F\u0915", "\u092F\u0939", "\u0938\u0947", "\u0915\u094B", "\u0907\u0938", "\u0915\u093F",
		"\u091C\u094B", "\u0915\u0930", "\u092E\u0947", "\u0928\u0947", "\u0924\u094B", "\u0939\u0940", "\u092F\u093E", "\u0939\u094B",
		"\u0925\u093E", "\u0924\u0915", "\u0906\u092A", "\u092F\u0947", "\u0925\u0947", "\u0926\u094B", "\u0935\u0947", "\u0925\u0940",
		"\u091C\u093E", "\u0928\u093E", "\u0909\u0938", "\u090F\u0938", "\u092A\u0947", "\u0909\u0928", "\u0938\u094B", "\u092D\u0940",
		"\u0914\u0930", "\u0918\u0930", "\u0924\u092C", "\u091C\u092C", "\u0905\u0924", "\u0935", "\u0928" 
	};

	public static final String[] STOP_WORDS_EGNLISH = {
		"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself",
		"she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom",
		"this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",
		"did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about",
		"against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off",
		"over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few",
		"more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will",
		"just", "don't", "should", "now", "i'm", "i'll", "you're", "you'll", "it's", "we're", "they're", "can't", "doesn't", "didn't", "oh", "ah",
		"i'd", "couldn't", "wouldn't", "he's", "she's", "i've", "you've", "he'll", "she'll", "we'll", "they'll", "it'll", "there's", "here's",
		"won't", "shouldn't", "haven't", "hasn't", "hadn't", "there're", "here're"
	};

	public static final String[] STOP_WORDS_FINNISH = {
		"olla", "olen", "olet", "olemme", "olette", "ovat", "ole", "oli", "olisi", "olisit", "olisin", "olisimme", "olisitte", "olisivat", "olit",
		"olin", "olimme", "olitte", "olivat", "ollut", "olleet", "en", "et", "ei", "emme", "ette", "eiv\u00E4t", "min\u00E4", "minun", "minua",
		"minussa", "minusta", "minuun", "minulla", "minulta", "minulle", "sin\u00E4", "sinun", "sinut", "sinua", "sinussa", "sinusta", "sinuun",
		"sinulla", "sinulta", "sinulle", "h\u00E4n", "h\u00E4nen", "h\u00E4net", "h\u00E4nt\u00E4", "h\u00E4ness\u00E4", "h\u00E4nest\u00E4",
		"h\u00E4neen", "h\u00E4nell\u00E4", "h\u00E4nelt\u00E4", "h\u00E4nelle", "meid\u00E4n", "meid\u00E4t", "meit\u00E4", "meiss\u00E4",
		"meist\u00E4", "meihin", "meill\u00E4", "meilt\u00E4", "meille", "te", "teid\u00E4n", "teid\u00E4t", "teit\u00E4", "teiss\u00E4",
		"teist\u00E4", "teihin", "teill\u00E4", "teilt\u00E4", "teille", "heid\u00E4n", "heid\u00E4t", "heit\u00E4", "heiss\u00E4", "heist\u00E4",
		"heihin", "heill\u00E4", "heilt\u00E4", "heille", "t\u00E4m\u00E4", "t\u00E4m\u00E4n", "t\u00E4t\u00E4", "t\u00E4ss\u00E4",
		"t\u00E4st\u00E4", "t\u00E4h\u00E4n", "tall\u00E4", "t\u00E4lt\u00E4", "t\u00E4lle", "t\u00E4n\u00E4", "t\u00E4ksi", "tuo", "tuon",
		"tuot\u00E4", "tuossa", "tuosta", "tuohon", "tuolla", "tuolta", "tuolle", "tuona", "tuoksi", "se", "sit\u00E4", "siin\u00E4", "siit\u00E4",
		"siihen", "sill\u00E4", "silt\u00E4", "sille", "sin\u00E4", "siksi", "n\u00E4m\u00E4", "n\u00E4iden", "n\u00E4it\u00E4", "n\u00E4iss\u00E4",
		"n\u00E4ist\u00E4", "n\u00E4ihin", "n\u00E4ill\u00E4", "n\u00E4ilt\u00E4", "n\u00E4ille", "n\u00E4in\u00E4", "n\u00E4iksi", "nuo", "noiden",
		"noita", "noissa", "noista", "noihin", "noilla", "noilta", "noille", "noina", "noiksi", "ne", "niiden", "niit\u00E4", "niiss\u00E4",
		"niist\u00E4", "niihin", "niill\u00E4", "niilt\u00E4", "niille", "niin\u00E4", "niiksi", "kuka", "kenen", "kenet", "ket\u00E4",
		"keness\u00E4", "kenest\u00E4", "keneen", "kenell\u00E4", "kenelt\u00E4", "kenelle", "kenen\u00E4", "keneksi", "ketk\u00E4", "keiden",
		"ketk\u00E4", "keit\u00E4", "keiss\u00E4", "keist\u00E4", "keihin", "keill\u00E4", "keilt\u00E4", "keille", "kein\u00E4", "keiksi",
		"mik\u00E4", "mink\u00E4", "mink\u00E4", "mit\u00E4", "miss\u00E4", "mist\u00E4", "mihin", "mill\u00E4", "milt\u00E4", "mille", "min\u00E4",
		"miksi", "mitk\u00E4", "joka", "jonka", "jossa", "josta", "johon", "jolla", "jolta", "jolle", "jona", "joksi", "jotka", "joiden", "joita",
		"joissa", "joista", "joihin", "joilla", "joilta", "joille", "joina", "joiksi", "ett\u00E4", "ja", "jos", "koska", "kuin", "mutta", "niin",
		"sek\u00E4", "sill\u00E4", "tai", "vaan", "vai", "vaikka", "kanssa", "mukaan", "noin", "poikki", "yli", "kun", "niin", "nyt", "itse" 
	};

	public static final String[] STOP_WORDS_DANISH = {
		"og", "jeg", "det", "en", "til", "er", "som", "p\u00E5", "de", "med", "af", "ikke", "der", "var", "sig", "et", "har", "om", "havde", "hun",
		"nu", "da", "fra", "du", "ud", "dem", "op", "hans", "hvor", "eller", "hvad", "skal", "selv", "alle", "vil", "blev", "kunne", "ind",
		"n\u00E5r", "v\u00E6re", "noget", "ville", "jo", "deres", "efter", "ned", "skulle", "denne", "dette", "ogs\u00E5", "anden", "hende", "meget",
		"vor", "disse", "hvis", "nogle", "blive", "bliver", "hendes", "v\u00E6ret", "thi", "jer", "s\u00E5dan" 
	};

	public static final String[] STOP_WORDS_ARMENIAN = {
		"\u0561\u0575\u0564", "\u0561\u0575\u056C", "\u0561\u0575\u0576", "\u0561\u0575\u057D", "\u0564\u0578\u0582", "\u0564\u0578\u0582\u0584",
		"\u0565\u0574", "\u0565\u0576", "\u0565\u0576\u0584", "\u0565\u057D", "\u0565\u0584", "\u0567", "\u0567\u056B", "\u0567\u056B\u0576",
		"\u0567\u056B\u0576\u0584", "\u0567\u056B\u0580", "\u0567\u056B\u0584", "\u0567\u0580", "\u0568\u057D\u057F", "\u0569", "\u056B",
		"\u056B\u0576", "\u056B\u057D\u056F", "\u056B\u0580", "\u056F\u0561\u0574", "\u0570\u0561\u0574\u0561\u0580", "\u0570\u0565\u057F",
		"\u0570\u0565\u057F\u0578", "\u0574\u0565\u0576\u0584", "\u0574\u0565\u057B", "\u0574\u056B", "\u0576", "\u0576\u0561", "\u0576\u0561\u0587",
		"\u0576\u0580\u0561", "\u0576\u0580\u0561\u0576\u0584", "\u0578\u0580", "\u0578\u0580\u0568", "\u0578\u0580\u0578\u0576\u0584",
		"\u0578\u0580\u057A\u0565\u057D", "\u0578\u0582", "\u0578\u0582\u0574", "\u057A\u056B\u057F\u056B", "\u057E\u0580\u0561", "\u0587" 
	};

	public static final String[] STOP_WORDS_BRAZILIAN = {
		"ainda", "alem", "ambas", "ao", "aonde", "aos", "apos", "aquele", "aqueles", "assim", "com", "como", "contra", "contudo", "cuja","cujas",
		"cujo", "cujos", "da", "das", "de", "dela", "dele", "deles", "demais", "depois", "desde", "desta", "deste", "dispoe", "dispoem", "diversa",
		"diversas", "diversos", "dos", "durante", "e", "ela", "elas", "ele", "eles", "em", "entao", "entre", "essa", "essas", "esse", "esses",
		"esta", "estas", "este", "estes", "ha", "isso", "isto", "mais", "mas", "mediante", "menos", "mesma", "mesmas", "mesmo", "mesmos", "na",
		"nas", "nao", "nas", "nem", "nesse", "neste", "nos", "o", "ou", "outra", "outras", "outro", "outros", "pelas", "pelo", "pelos", "perante",
		"pois", "por", "porque", "portanto", "proprio", "propios", "quais", "qualquer", "quando", "quanto", "que", "quem", "quer", "se", "seja",
		"sem", "sendo", "seu", "seus", "sob", "sobre", "sua", "suas", "tal", "tambem", "teu", "teus", "toda", "todas", "todos", "tua", "tuas",
		"tudo","um","uma","umas","uns"
	};

	public static final String[] STOP_WORDS_CZECH = {
		"a", "s", "k", "o", "i", "u", "v", "z", "dnes", "cz", "t\u00edmto", "bude\u0161", "budem", "byli", "jse\u0161", "m\u016fj", "sv\u00fdm",
		"ta", "tomto", "tohle", "tuto", "tyto", "jej", "zda", "pro\u010d", "m\u00e1te", "tato", "kam", "tohoto", "kdo", "kte\u0159\u00ed", "mi",
		"n\u00e1m", "tom", "tomuto", "m\u00edt", "nic", "proto", "kterou", "byla", "toho", "proto\u017ee", "asi", "ho", "na\u0161i", "napi\u0161te",
		"re", "co\u017e", "t\u00edm", "tak\u017ee", "sv\u00fdch", "jej\u00ed", "sv\u00fdmi", "jste", "aj", "tu", "tedy", "teto", "bylo", "kde", "ke",
		"prav\u00e9", "ji", "nad", "nejsou", "\u010di", "pod", "t\u00e9ma", "mezi", "p\u0159es", "ty", "pak", "v\u00e1m", "ani", "kdy\u017e",
		"v\u0161ak", "neg", "jsem", "tento", "\u010dl\u00e1nku", "\u010dl\u00e1nky", "aby", "jsme", "p\u0159ed", "pta", "jejich", "byl",
		"je\u0161t\u011b", "a\u017e", "bez", "tak\u00e9", "pouze", "prvn\u00ed", "va\u0161e", "kter\u00e1", "n\u00e1s", "nov\u00fd", "tipy", "pokud",
		"m\u016f\u017ee", "strana", "jeho", "sv\u00e9", "jin\u00e9", "zpr\u00e1vy", "nov\u00e9", "nen\u00ed", "v\u00e1s", "jen", "podle", "zde",
		"u\u017e", "b\u00fdt", "v\u00edce", "bude", "ji\u017e", "ne\u017e", "kter\u00fd", "by", "kter\u00e9", "co", "nebo", "tak", "m\u00e1",
		"p\u0159i", "od", "po", "jsou", "jak", "dal\u0161\u00ed", "ale", "si", "se", "ve", "jako", "za", "zp\u011bt", "ze", "je", "na", "atd", "atp",
		"jakmile", "p\u0159i\u010dem\u017e", "j\u00e1", "ona", "ono", "oni", "ony", "vy", "j\u00ed", "ji", "m\u011b", "mne", "jemu", "tomu",
		"t\u011bm", "t\u011bmu", "n\u011bmu", "n\u011bmu\u017e", "jeho\u017e", "j\u00ed\u017e", "jeliko\u017e", "je\u017e", "jako\u017e",
		"na\u010de\u017e"
	};
}

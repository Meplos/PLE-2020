package bigdata;

import org.apache.hadoop.util.ProgramDriver;

public class Patterns {
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			/*pgd.addClass("rsjoin", bigdata.Joiner.class, "make a reduce-side join : cities regions result");
			pgd.addClass("sortedjoin", bigdata.SortedJoiner.class, "make a sorted reduce-side join : cities regions result");
			pgd.addClass("topk", bigdata.TopK.class, "find top k elements : k input output");*/
			pgd.addClass("wordEvolution", bigdata.EvolutionHBase.class, "apparition du mot chaque jour : mot");
			pgd.addClass("hashtagEvolution", bigdata.HashtagPopu.class, "apparition des hastags chaque jour");
			pgd.addClass("RTtweet", bigdata.RTStats.class, "RTstatistique");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
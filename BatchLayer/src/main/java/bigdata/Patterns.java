package bigdata;

import org.apache.hadoop.util.ProgramDriver;

public class Patterns {
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("wordEvolution", bigdata.EvolutionHBase.class, "apparition du mot chaque jour : mot");
			pgd.addClass("hashtagEvolution", bigdata.HashtagPopu.class, "apparition des hastags chaque jour");
			pgd.addClass("TopKLanguage", bigdata.TopKLanguage.class, "TopKLanguage : k");
			pgd.addClass("EvolutionLang", bigdata.EvolutionLang.class, "Evolution du nombre de tweets par jour dans la langue en paramètre : lang");
			pgd.addClass("TopkLocation", bigdata.TopkLocation.class, "Topk des locations pour la langue en param : lang k");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
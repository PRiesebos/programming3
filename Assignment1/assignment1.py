from Bio import Entrez
from time import sleep
import argparse as ap
import multiprocessing as mp


def get_references(pubmed_id):
    Entrez.email = "pcriesebos@gmail.com"
    references = []

    results = Entrez.read(
        Entrez.elink(
            dbfrom="pubmed",
            db="pmc",
            LinkName="pubmed_pmc_refs",
            id=pubmed_id,
            api_key="1b128aaba77c37664c213d753017ca520108",
        )
    )

    references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]
    print(f"There are a total of {len(references)} references")

    return references


def write_to_xml(pubmed_id):
    handle = Entrez.efetch(
        db="pmc",
        id=pubmed_id,
        rettype="XML",
        retmode="text",
        api_key="1b128aaba77c37664c213d753017ca520108",
    )
    try:
        with open(f"output/{pubmed_id}.xml", "wb") as file:
            file.write(handle.read())
            file.close()
    except IOError:
        print("Something went wrong")
    sleep(15)


if __name__ == "__main__":
    argparser = ap.ArgumentParser(
        description="Script that downloads (default) 10 articles referenced by the given PubMed ID concurrently."
    )
    argparser.add_argument(
        "-n",
        action="store",
        dest="n",
        required=False,
        type=int,
        help="Number of references to download concurrently.",
    )
    argparser.add_argument(
        "pubmed_id",
        action="store",
        type=str,
        nargs=1,
        help="Pubmed ID of the article to harvest for references to download.",
    )

    args = argparser.parse_args()
    cpus = mp.cpu_count()
    count = args.n or 10
    ids = get_references(args.pubmed_id)[:count]

    print(f"Getting: {count} articles for {args.pubmed_id}")

    with mp.Pool(cpus) as pool:
        results = pool.map(write_to_xml, ids)

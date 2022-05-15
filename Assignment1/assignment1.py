import argparse as ap

handle = Entrez.efetch(
    db="pmc",
    id=pubmed_id,
    rettype="XML",
    retmode="text",
    api_key="1b128aaba77c37664c213d753017ca520108",
)
with open(f"output/{pubmed_id}.xml", "wb") as file:
    file.write(handle.read())

Entrez.email = "pcriesebos@gmail.com"
results = Entrez.read(
    Entrez.elink(
        dbfrom="pubmed",
        db="pmc",
        LinkName="pubmed_pmc_refs",
        id=pubmed_id,
        api_key="INSERT YOUR PUBMED API KEY HERE!",
    )
)
references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]


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
    print("Getting: ", args.pubmed_id)

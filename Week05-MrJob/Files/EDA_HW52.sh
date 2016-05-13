joinTable=$1

echo "Number of webpage IDs:                            "\
    $(cut -d, -f1 $joinTable | grep -v None | sort | uniq | wc -l)
echo "Number of webpage URLs:                           "\
    $(cut -d, -f1,2 $joinTable | grep -v None | sort  | uniq | wc -l)
echo "Number of webpages with no associated webpage URL:"\
    $(cut -d, -f1,2 $joinTable | grep None | sort | uniq | wc -l)
echo "Number of webpages visited:                       "\
    $(cut -d, -f1,3 $joinTable | grep -v None | cut -d, -f1 | sort | uniq | \
      wc -l)
echo "Number of records:                                "\
    $(wc -l < $joinTable)
echo "Number of visits:                                 "\
    $(cut -d, -f3 $joinTable | grep -v None | wc -l)
echo "Number of webpages with no associated visitor ID: "\
    $(cut -d, -f1,3 $joinTable | grep None | sort | uniq | wc -l)
echo "Number of visitors (IDs):                         "\
    $(cut -d, -f3 $joinTable | grep -v None | sort | uniq | wc -l)
if [ $(grep None $joinTable | wc -l) != 0 ]; then \
    echo -e "Webpages with no visits or URL:\n$(grep None $joinTable | \
    sort | sed 's/^/\t/')"; fi
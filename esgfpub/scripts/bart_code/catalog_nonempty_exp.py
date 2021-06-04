import os
import argparse

acomment = 'Hangs on Links'

help_text = 'Produce the list filecount,directorypath for all paths containing regular files\n\
(may hang if links are encountered?)'

parser = argparse.ArgumentParser(description=help_text)
parser.add_argument('--targetdir', "-t", dest='targetdir', help="directory to assess")

args = parser.parse_args()

src_selected = []
for root, dirs, files in os.walk(args.targetdir):      # aggregate full sourcefile paths in src_selected
    # if not dirs and (src_selector in root):     # at leaf-directory matching src_selector
    if not dirs:     # at leaf-directory matching src_selector
        src_selected.append(root)
        #for afile in files:
        #    src_selected.append(os.path.join(root,afile))

wh_nonempty = []
for adir in src_selected:
    # print(adir)
    for root, dirs, files in os.walk(adir):
        if files:
            wh_nonempty.append( tuple([adir,os.path.basename(adir),len(files)]))

for atup in wh_nonempty:
    atmp = '.'.join(atup[0].split('/')[5:])
    print(f'{atmp}')


# can use os.path.islink to test if "files" are actually "links"


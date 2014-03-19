package extract

import (
	"regexp"
	"strconv"
	"strings"
)

type releaseExtract struct {
	regex        *regexp.Regexp
	subjectIndex int
	usePartless  bool
}

var subjectMatchers map[string][]releaseExtract
var partLessA *regexp.Regexp
var partLessB *regexp.Regexp
var yencMatch *regexp.Regexp
var fileCountMatch *regexp.Regexp
var filenameMatch *regexp.Regexp

func init() {
	subjectMatchers = make(map[string][]releaseExtract)
	compileRegex()
	partLessA = regexp.MustCompile(`(\(\d+\/\d+\))?(\(\d+\/\d+\))?(\(\d+\/\d+\))?(\(\d+\/\d+\))?(\(\d+\/\d+\))?(\(\d+\/\d+\))?(\(\d+\/\d+\))?$`)
	partLessB = regexp.MustCompile(`(?i)yEnc.*?$`)

	// nzedb/Binaries.php
	yencMatch = regexp.MustCompile(`(.+yEnc)(\.\s*|\s*by xMas\s*|_|\s*--\s*READ NFO!\s*|\s*| \[S\d+E\d+\]|\s*".+"\s*)\((\d+)\/(\d+)\)`)
	fileCountMatch = regexp.MustCompile(`(\[|\(|\s)(\d{1,5})(\/|(\s|_)of(\s|_)|\-)(\d{1,5})(\]|\)|\s|$|:)`)

	filenameMatch = regexp.MustCompile(`(?i)"(.+)"`)
}

func addReleaseExtract(group string, regex string, subjectIndex int, usePartless bool) {
	r := releaseExtract{
		regex:        regexp.MustCompile(regex),
		subjectIndex: subjectIndex,
		usePartless:  usePartless,
	}
	if _, ok := subjectMatchers[group]; ok {
		subjectMatchers[group] = append(subjectMatchers[group], r)
	} else {
		subjectMatchers[group] = []releaseExtract{r}
	}
}

func compileRegex() {
	e0 := `([-_](proof|sample|thumbs?))*(\.part\d*(\.rar)?|\.rar)?(\d{1,3}\.rev"|\.vol.+?"|\.[A-Za-z0-9]{2,4}"|")`
	e1 := e0 + ` yEnc$`
	//([AST] One Piece Episode 301-350 [720p]) [007/340] - "One Piece episode 301-350.part006.rar" yEnc
	addReleaseExtract("alt.binaries.anime", `^\((\[.+?\] .+?)\) \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[REPOST][ New Doraemon 2013.05.03 Episode 328 (TV Asahi) 1080i HDTV MPEG2 AAC-DoraClub.org ] [35/61] - "doraclub.org-doraemon-20130503-b8de1f8e.r32" yEnc
	addReleaseExtract("alt.binaries.anime", `^\[.+?\]\[ (.+?) \] \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[De.us] Suzumiya Haruhi no Shoushitsu (1920x1080 h.264 Dual-Audio FLAC 10-bit) [017CB24D] [000/357] - "[De.us] Suzumiya Haruhi no Shoushitsu (1920x1080 h.264 Dual-Audio FLAC 10-bit) [017CB24D].nzb" yEnc
	addReleaseExtract("alt.binaries.anime", `^(\[.+?\] [.+?] \[[A-F0-9]+\]) \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[eraser] Ghost in the Shell ARISE - border_1 Ghost Pain (BD 720p Hi444PP LC-AAC Stereo) - [01/65] - "[eraser] Ghost in the Shell ARISE - border_1 Ghost Pain (BD 720p Hi444PP LC-AAC Stereo) .md5" yEnc
	addReleaseExtract("alt.binaries.anime", `^\[.+?\] (.+?) - \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//(01/27) - Maid.Sama.Jap.dubbed.german.english.subbed - "01 Misaki ist eine Maid!.divx" - 6,44 GB - yEnc
	addReleaseExtract("alt.binaries.anime", `^\(\d+\/\d+\) - (.+?) - ".+?" - \d+[,.]\d+ [mMkKgG][bB] - yEnc$`, 1, true)
	//[ New Doraemon 2013.06.14 Episode 334 (TV Asahi) 1080i HDTV MPEG2 AAC-DoraClub.org ] [01/60] - "doraclub.org-doraemon-20130614-fae28cec.nfo" yEnc
	addReleaseExtract("alt.binaries.anime", `^\[ (.+?) \] \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//<TOWN> www.town.ag > sponsored by www.ssl-news.info > (1/3) "HolzWerken_40.par2" - 43,89 MB - yEnc
	addReleaseExtract("alt.binaries.anime", `^<TOWN> www\.town\.ag > sponsored by www\.ssl-news\.info > \(\d+\/\d+\) "(.+?)`+e0+` - \d+[,.]\d+ [mMkKgG][bB] - yEnc$`, 1, true)
	//(1/9)<<<www.town.ag>>> sponsored by ssl-news.info<<<[HorribleSubs]_AIURA_-_01_[480p].mkv "[HorribleSubs]_AIURA_-_01_[480p].par2" yEnc
	addReleaseExtract("alt.binaries.anime", `^\(\d+\/\d+\).+?www\.town\.ag.+?sponsored by (www\.)?ssl-news\.info<+?.+? "(.+?)`+e1, 2, true)
	//Overman King Gainer [Dual audio, EngSub] Exiled Destiny - [002/149] - "Overman King Gainer.part001.rar" yEnc
	addReleaseExtract("alt.binaries.anime", `^(.+? \[Dual [aA]udio, EngSub\] .+?) - \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[ TOWN ]-[ www.town.ag ]-[ partner of www.ssl-news.info ]-[ MOVIE ] [14/19] - "Night.Vision.2011.DVDRip.x264-IGUANA.part12.rar" - 660,80 MB yEnc
	addReleaseExtract("alt.binaries.anime", `(?i)^\[ TOWN \][ _-]{0,3}\[ www\.town\.ag \][ _-]{0,3}\[ partner of www\.ssl-news\.info \][ _-]{0,3}\[ .* \] \[\d+\/\d+\][ _-]{0,3}("|#34;)(.+)((\.part\d+\.rar)|(\.vol\d+\+\d+\.par2))("|#34;)[ _-]{0,3}\d+[.,]\d+ [kKmMgG][bB][ _-]{0,3}yEnc$`, 2, true)
	//[ TOWN ]-[ www.town.ag ]-[ partner of www.ssl-news.info ]-[ MOVIE ] [01/84] - "The.Butterfly.Effect.2.2006.1080p.BluRay.x264-LCHD.par2" - 7,49 GB yEnc
	addReleaseExtract("alt.binaries.anime", `(?i)^\[ TOWN \][ _-]{0,3}\[ www\.town\.ag \][ _-]{0,3}\[ partner of www\.ssl-news\.info \][ _-]{0,3}\[ .* \] \[\d+\/\d+\][ _-]{0,3}("|#34;)(.+)\.(par2|rar|nfo|nzb)("|#34;)[ _-]{0,3}\d+[.,]\d+ [kKmMgG][bB][ _-]{0,3}yEnc$`, 2, true)
	addReleaseExtract("alt.binaries.anime", "(?i)^.*?\\.mkv.*?\"(?P<name>.*?)\\.mkv", 1, false)
	addReleaseExtract("alt.binaries.anime", "(?i)^(?P<name>.*?\\]) \\[(?P<parts>\\d{1,3}\\/\\d{1,3})", 1, false)
	addReleaseExtract("alt.binaries.anime", "(?i)^.*?\"(?P<name>.*?(\\]|\\)))\\.", 1, false)

	//High School DxD New 01 (480p|.avi|xvid|mp3) ~bY Hatsuyuki [01/18] - "[Hatsuyuki]_High_School_DxD_New_01_[848x480][76B2BB8C].avi.001" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `.+? \((360|480|720|1080)p\|.+? ~bY .+? \[\d+\/\d+\] - "(.+?\[[A-F0-9]+\].+?)`+e1, 2, true)
	//([AST] One Piece Episode 301-350 [720p]) [007/340] - "One Piece episode 301-350.part006.rar" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `^\((\[.+?\] .+?)\) \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[REPOST][ New Doraemon 2013.05.03 Episode 328 (TV Asahi) 1080i HDTV MPEG2 AAC-DoraClub.org ] [35/61] - "doraclub.org-doraemon-20130503-b8de1f8e.r32" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `^\[.+?\]\[ (.+?) \] \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[De.us] Suzumiya Haruhi no Shoushitsu (1920x1080 h.264 Dual-Audio FLAC 10-bit) [017CB24D] [000/357] - "[De.us] Suzumiya Haruhi no Shoushitsu (1920x1080 h.264 Dual-Audio FLAC 10-bit) [017CB24D].nzb" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `^(\[.+?\] [.+?] \[[A-F0-9]+\]) \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[eraser] Ghost in the Shell ARISE - border_1 Ghost Pain (BD 720p Hi444PP LC-AAC Stereo) - [01/65] - "[eraser] Ghost in the Shell ARISE - border_1 Ghost Pain (BD 720p Hi444PP LC-AAC Stereo) .md5" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `^\[.+?\] (.+?) - \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//(01/27) - Maid.Sama.Jap.dubbed.german.english.subbed - "01 Misaki ist eine Maid!.divx" - 6,44 GB - yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `^\(\d+\/\d+\) - (.+?) - ".+?" - \d+[,.]\d+ [mMkKgG][bB] - yEnc$`, 1, true)
	//[ New Doraemon 2013.06.14 Episode 334 (TV Asahi) 1080i HDTV MPEG2 AAC-DoraClub.org ] [01/60] - "doraclub.org-doraemon-20130614-fae28cec.nfo" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `^\[ (.+?) \] \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//<TOWN> www.town.ag > sponsored by www.ssl-news.info > (1/3) "HolzWerken_40.par2" - 43,89 MB - yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `^<TOWN> www\.town\.ag > sponsored by www\.ssl-news\.info > \(\d+\/\d+\) "(.+?)`+e0+` - \d+[,.]\d+ [mMkKgG][bB] - yEnc$`, 1, true)
	//(1/9)<<<www.town.ag>>> sponsored by ssl-news.info<<<[HorribleSubs]_AIURA_-_01_[480p].mkv "[HorribleSubs]_AIURA_-_01_[480p].par2" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `^\(\d+\/\d+\).+?www\.town\.ag.+?sponsored by (www\.)?ssl-news\.info<+?.+? "(.+?)`+e1, 2, true)
	//Overman King Gainer [Dual audio, EngSub] Exiled Destiny - [002/149] - "Overman King Gainer.part001.rar" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `^(.+? \[Dual [aA]udio, EngSub\] .+?) - \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[ TOWN ]-[ www.town.ag ]-[ partner of www.ssl-news.info ]-[ MOVIE ] [14/19] - "Night.Vision.2011.DVDRip.x264-IGUANA.part12.rar" - 660,80 MB yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `(?i)^\[ TOWN \][ _-]{0,3}\[ www\.town\.ag \][ _-]{0,3}\[ partner of www\.ssl-news\.info \][ _-]{0,3}\[ .* \] \[\d+\/\d+\][ _-]{0,3}("|#34;)(.+)((\.part\d+\.rar)|(\.vol\d+\+\d+\.par2))("|#34;)[ _-]{0,3}\d+[.,]\d+ [kKmMgG][bB][ _-]{0,3}yEnc$`, 2, true)
	//[ TOWN ]-[ www.town.ag ]-[ partner of www.ssl-news.info ]-[ MOVIE ] [01/84] - "The.Butterfly.Effect.2.2006.1080p.BluRay.x264-LCHD.par2" - 7,49 GB yEnc
	addReleaseExtract("alt.binaries.multimedia.anime", `(?i)^\[ TOWN \][ _-]{0,3}\[ www\.town\.ag \][ _-]{0,3}\[ partner of www\.ssl-news\.info \][ _-]{0,3}\[ .* \] \[\d+\/\d+\][ _-]{0,3}("|#34;)(.+)\.(par2|rar|nfo|nzb)("|#34;)[ _-]{0,3}\d+[.,]\d+ [kKmMgG][bB][ _-]{0,3}yEnc$`, 2, true)
	addReleaseExtract("alt.binaries.multimedia.anime", "(?i)^.*?\\.mkv.*?\"(?P<name>.*?)\\.mkv", 1, false)
	addReleaseExtract("alt.binaries.multimedia.anime", "(?i)^(?P<name>.*?\\]) \\[(?P<parts>\\d{1,3}\\/\\d{1,3})", 1, false)
	addReleaseExtract("alt.binaries.multimedia.anime", "(?i)^.*?\"(?P<name>.*?(\\]|\\)))\\.", 1, false)

	//High School DxD New 01 (480p|.avi|xvid|mp3) ~bY Hatsuyuki [01/18] - "[Hatsuyuki]_High_School_DxD_New_01_[848x480][76B2BB8C].avi.001" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `.+? \((360|480|720|1080)p\|.+? ~bY .+? \[\d+\/\d+\] - "(.+?\[[A-F0-9]+\].+?)`+e1, 2, true)
	//([AST] One Piece Episode 301-350 [720p]) [007/340] - "One Piece episode 301-350.part006.rar" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `^\((\[.+?\] .+?)\) \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[REPOST][ New Doraemon 2013.05.03 Episode 328 (TV Asahi) 1080i HDTV MPEG2 AAC-DoraClub.org ] [35/61] - "doraclub.org-doraemon-20130503-b8de1f8e.r32" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `^\[.+?\]\[ (.+?) \] \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[De.us] Suzumiya Haruhi no Shoushitsu (1920x1080 h.264 Dual-Audio FLAC 10-bit) [017CB24D] [000/357] - "[De.us] Suzumiya Haruhi no Shoushitsu (1920x1080 h.264 Dual-Audio FLAC 10-bit) [017CB24D].nzb" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `^(\[.+?\] [.+?] \[[A-F0-9]+\]) \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[eraser] Ghost in the Shell ARISE - border_1 Ghost Pain (BD 720p Hi444PP LC-AAC Stereo) - [01/65] - "[eraser] Ghost in the Shell ARISE - border_1 Ghost Pain (BD 720p Hi444PP LC-AAC Stereo) .md5" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `^\[.+?\] (.+?) - \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//(01/27) - Maid.Sama.Jap.dubbed.german.english.subbed - "01 Misaki ist eine Maid!.divx" - 6,44 GB - yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `^\(\d+\/\d+\) - (.+?) - ".+?" - \d+[,.]\d+ [mMkKgG][bB] - yEnc$`, 1, true)
	//[ New Doraemon 2013.06.14 Episode 334 (TV Asahi) 1080i HDTV MPEG2 AAC-DoraClub.org ] [01/60] - "doraclub.org-doraemon-20130614-fae28cec.nfo" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `^\[ (.+?) \] \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//<TOWN> www.town.ag > sponsored by www.ssl-news.info > (1/3) "HolzWerken_40.par2" - 43,89 MB - yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `^<TOWN> www\.town\.ag > sponsored by www\.ssl-news\.info > \(\d+\/\d+\) "(.+?)`+e0+` - \d+[,.]\d+ [mMkKgG][bB] - yEnc$`, 1, true)
	//(1/9)<<<www.town.ag>>> sponsored by ssl-news.info<<<[HorribleSubs]_AIURA_-_01_[480p].mkv "[HorribleSubs]_AIURA_-_01_[480p].par2" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `^\(\d+\/\d+\).+?www\.town\.ag.+?sponsored by (www\.)?ssl-news\.info<+?.+? "(.+?)`+e1, 2, true)
	//Overman King Gainer [Dual audio, EngSub] Exiled Destiny - [002/149] - "Overman King Gainer.part001.rar" yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `^(.+? \[Dual [aA]udio, EngSub\] .+?) - \[\d+\/\d+\] - ".+?" yEnc$`, 1, true)
	//[ TOWN ]-[ www.town.ag ]-[ partner of www.ssl-news.info ]-[ MOVIE ] [14/19] - "Night.Vision.2011.DVDRip.x264-IGUANA.part12.rar" - 660,80 MB yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `(?i)^\[ TOWN \][ _-]{0,3}\[ www\.town\.ag \][ _-]{0,3}\[ partner of www\.ssl-news\.info \][ _-]{0,3}\[ .* \] \[\d+\/\d+\][ _-]{0,3}("|#34;)(.+)((\.part\d+\.rar)|(\.vol\d+\+\d+\.par2))("|#34;)[ _-]{0,3}\d+[.,]\d+ [kKmMgG][bB][ _-]{0,3}yEnc$`, 2, true)
	//[ TOWN ]-[ www.town.ag ]-[ partner of www.ssl-news.info ]-[ MOVIE ] [01/84] - "The.Butterfly.Effect.2.2006.1080p.BluRay.x264-LCHD.par2" - 7,49 GB yEnc
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", `(?i)^\[ TOWN \][ _-]{0,3}\[ www\.town\.ag \][ _-]{0,3}\[ partner of www\.ssl-news\.info \][ _-]{0,3}\[ .* \] \[\d+\/\d+\][ _-]{0,3}("|#34;)(.+)\.(par2|rar|nfo|nzb)("|#34;)[ _-]{0,3}\d+[.,]\d+ [kKmMgG][bB][ _-]{0,3}yEnc$`, 2, true)
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", "(?i)^.*?\\.mkv.*?\"(?P<name>.*?)\\.mkv", 1, false)
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", "(?i)^(?P<name>.*?\\]) \\[(?P<parts>\\d{1,3}\\/\\d{1,3})", 1, false)
	addReleaseExtract("alt.binaries.multimedia.anime.highspeed", "(?i)^.*?\"(?P<name>.*?(\\]|\\)))\\.", 1, false)
}

func partLess(subject string) string {
	r := partLessA.ReplaceAllString(subject, "yEnc")
	r = partLessB.ReplaceAllString(r, "yEnc")
	return strings.TrimSpace(r)
}

func ExtractRelease(group string, subject string) string {
	partlessSubject := partLess(subject)
	if sm, ok := subjectMatchers[group]; ok {
		for _, m := range sm {
			s := subject
			if m.usePartless {
				s = partlessSubject
			}
			if res := m.regex.FindStringSubmatch(s); res != nil {
				return res[m.subjectIndex]
			}
		}
	}
	return ""
}

func ExtractFile(subject string) string {
	if res := filenameMatch.FindStringSubmatch(subject); res != nil {
		return res[1]
	}
	return ""
}

func ExtractYencPart(subject string) int {
	if res := yencMatch.FindStringSubmatch(subject); res != nil {
		if r, e := strconv.Atoi(res[3]); e == nil {
			return r
		}
	}
	return 1
}

func ExtractYencLength(subject string) int {
	if res := yencMatch.FindStringSubmatch(subject); res != nil {
		if r, e := strconv.Atoi(res[4]); e == nil {
			return r
		}
	}
	return 1
}

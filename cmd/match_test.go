package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_wikiEntry(t *testing.T) {
	var entry *wikiEntry
	require.False(t, entry.isMovie())
}

func Test_JSONDecode(t *testing.T) {
	in := `[{"name": "jason"}, {"name": "akkash", "job": "bob"}]`
	// in := `[{'credit_id': '570b92e1c3a368757000752f', 'department': 'Sound', 'gender': 2, 'id': 405, 'job': 'Original Music Composer', 'name': 'Alberto Iglesias', 'profile_path': '/2PGZyiqdAAyi2S0ZpFw5V0ojCqY.jpg'}, {'credit_id': '537cc0cd0e0a263162000623', 'department': 'Art', 'gender': 1, 'id': 471, 'job': 'Production Design', 'name': 'Maria Djurkovic', 'profile_path': None}, {'credit_id': '537cc0780e0a262a970019a2', 'department': 'Production', 'gender': 1, 'id': 474, 'job': 'Casting', 'name': 'Jina Jay', 'profile_path': '/rMuj07hjZnT0zMC1kiBOs6IWCdO.jpg'}, {'credit_id': '52fe4799c3a36847f813e4a9', 'department': 'Production', 'gender': 2, 'id': 2236, 'job': 'Producer', 'name': 'Tim Bevan', 'profile_path': '/f7o93O1KocuLwIrSa7KqyL1sWaT.jpg'}, {'credit_id': '52fe4799c3a36847f813e4af', 'department': 'Production', 'gender': 2, 'id': 2238, 'job': 'Producer', 'name': 'Eric Fellner', 'profile_path': '/DiDxNhIfVPn9bRdOumhK0LgCYT.jpg'}, {'credit_id': '52fe4799c3a36847f813e4b5', 'department': 'Production', 'gender': 0, 'id': 15540, 'job': 'Producer', 'name': 'Robyn Slovo', 'profile_path': '/jxOpMb63IorZ3uFGfJXWwGbyDyt.jpg'}, {'credit_id': '52fe4798c3a36847f813e44b', 'department': 'Writing', 'gender': 0, 'id': 20422, 'job': 'Novel', 'name': 'John le Carré', 'profile_path': None}, {'credit_id': '52fe4799c3a36847f813e4c5', 'department': 'Costume & Make-Up', 'gender': 1, 'id': 36591, 'job': 'Costume Design', 'name': 'Jacqueline Durran', 'profile_path': '/8BzF1gUzKRiZKjjoQm2I50ALDSq.jpg'}, {'credit_id': '570b92749251415248001755', 'department': 'Art', 'gender': 2, 'id': 49345, 'job': 'Supervising Art Director', 'name': 'Mark Raggett', 'profile_path': None}, {'credit_id': '52fe4798c3a36847f813e451', 'department': 'Writing', 'gender': 2, 'id': 64814, 'job': 'Screenplay', 'name': 'Peter Straughan', 'profile_path': None}, {'credit_id': '52fe4798c3a36847f813e445', 'department': 'Directing', 'gender': 2, 'id': 74396, 'job': 'Director', 'name': 'Tomas Alfredson', 'profile_path': '/sRl6QzCtO7SlLspfFSvnv8u7ITi.jpg'}, {'credit_id': '52fe4799c3a36847f813e4bf', 'department': 'Camera', 'gender': 0, 'id': 74401, 'job': 'Director of Photography', 'name': 'Hoyte van Hoytema', 'profile_path': '/dT5fWu3M9qJQlE5rItpSGUUOMC0.jpg'}, {'credit_id': '537cc0b60e0a262a8b001914', 'department': 'Editing', 'gender': 0, 'id': 74403, 'job': 'Editor', 'name': 'Dino Jonsäter', 'profile_path': None}, {'credit_id': '570b92ce9251413627003013', 'department': 'Sound', 'gender': 2, 'id': 223202, 'job': 'Music Supervisor', 'name': 'Nick Angel', 'profile_path': None}, {'credit_id': '52fe4798c3a36847f813e457', 'department': 'Writing', 'gender': 1, 'id': 227094, 'job': 'Screenplay', 'name': "Bridget O'Connor", 'profile_path': None}, {'credit_id': '570b928592514111f2002993', 'department': 'Art', 'gender': 2, 'id': 1322142, 'job': 'Supervising Art Director', 'name': 'Tom Brown', 'profile_path': None}, {'credit_id': '537cc2110e0a262a88001a3e', 'department': 'Art', 'gender': 0, 'id': 1322144, 'job': 'Art Direction', 'name': 'Pilar Foy', 'profile_path': None}, {'credit_id': '537cc2340e0a262a840019a5', 'department': 'Art', 'gender': 0, 'id': 1322145, 'job': 'Set Decoration', 'name': 'Tatiana MacDonald', 'profile_path': None}, {'credit_id': '537cc2c70e0a262a840019be', 'department': 'Costume & Make-Up', 'gender': 0, 'id': 1322147, 'job': 'Costume Supervisor', 'name': 'Dan Grace', 'profile_path': None}, {'credit_id': '570b92a1c3a368757000751b', 'department': 'Directing', 'gender': 0, 'id': 1367566, 'job': 'Script Supervisor', 'name': 'Libbie Barr', 'profile_path': None}]`
	in = strings.ReplaceAll(in, `'`, `"`)
	in = strings.ReplaceAll(in, `None`, `""`)
	dec := json.NewDecoder(strings.NewReader(in))
	// token, err := dec.Token()
	// require.NoError(t, err)
	// fmt.Println(token)

	for {
		token, err := dec.Token()
		if err == io.EOF {
			fmt.Println("EOF")
			break
		}
		fmt.Println(token)
	}

	require.FailNow(t, "")
}

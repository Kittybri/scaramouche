import asyncio
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from memory import Memory
from restoration_store import RestorationStore, StaleCampaign
from harbinger_commands import HarbingerController, QuestView, parse_scenario


def run(fn):
    import functools
    @functools.wraps(fn)
    def wrapped(*a,**kw): return asyncio.run(fn(*a,**kw))
    return wrapped

async def setup(tmp_path):
    mem=Memory('scaramouche',str(tmp_path/'local.db'),str(tmp_path/'shared.db'))
    await mem.init()
    store=RestorationStore(mem.db_path)
    controller=HarbingerController(None,store,None,'fake',AsyncMock(return_value=False),lambda t:False)
    return mem,store,controller

@run
async def test_user_guild_isolation_and_reset_revokes_old_updates(tmp_path):
    mem,s,c=await setup(tmp_path)
    rev,state=await s.open(1,10)
    other=await s.open(2,10)
    elsewhere=await s.open(1,20)
    await s.forget(1)
    with pytest.raises(StaleCampaign): await s.update(1,10,rev,state)
    assert await s.get(2,10)==other
    assert await s.get(1,20) is None
    new,_=await s.open(1,10)
    assert new!=rev
    with pytest.raises(StaleCampaign): await s.reset(1,10,rev)
    with sqlite3.connect(mem.db_path) as db: assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok'

@run
async def test_duplicate_claims_and_medals_are_atomic(tmp_path):
    _,s,c=await setup(tmp_path)
    rev,state=await s.open(1,10)
    state['total_points']=99
    results=await asyncio.gather(s.update(1,10,rev,state,completed=True),s.update(1,10,rev,state,completed=True),return_exceptions=True)
    assert sum(isinstance(x,StaleCampaign) for x in results)==1
    assert await s.leaderboard(10,1)==[(1,1,99)]
    assert await s.leaderboard(20,1)==[]
    assert await s.leaderboard(0,2)==[]

@run
async def test_deletion_during_generation_never_recreates_state_or_sends(tmp_path):
    _,s,c=await setup(tmp_path)
    rev,state=await s.open(1,10)
    for choice in ['takeover','teyvat','pyro']:
        rev,state=await c.advance(1,10,rev,state,choice)
    async def generate(prompt):
        await s.forget(1)
        return json.dumps({'scenario':'safe marker','choices':[{'label':'x','points':p,'result':'safe'} for p in [0,1,3]]})
    c.generate=generate
    channel=SimpleNamespace(id=3,send=AsyncMock())
    with pytest.raises(StaleCampaign): await c.show(channel,1,10,rev,state)
    channel.send.assert_not_awaited()
    assert await s.get(1,10) is None

@run
async def test_wrong_user_and_wrong_channel_buttons_rejected(tmp_path):
    _,s,c=await setup(tmp_path)
    rev,state=await s.open(1,10)
    view=QuestView(c,1,10,30,rev,state)
    for uid,ch in [(2,30),(1,31)]:
        i=SimpleNamespace(user=SimpleNamespace(id=uid),guild_id=10,channel_id=ch,response=SimpleNamespace(send_message=AsyncMock()))
        await view.choose(i,'horror')
        i.response.send_message.assert_awaited_once()
    assert not view.claimed
    assert await s.get(1,10)==(rev,state)

@run
async def test_full_eleven_boss_campaign_preserves_dice_and_medal(tmp_path,monkeypatch):
    _,s,c=await setup(tmp_path)
    c.generate=AsyncMock(return_value='Victory.')
    monkeypatch.setattr('harbinger_commands.random.randint',lambda a,b:20)
    rev,state=await s.open(1,10)
    for choice in ['horror','transmigrated']:
        rev,state=await c.advance(1,10,rev,state,choice)
    for boss in range(11):
        for turn in range(9):
            state['scenario_data']={'choices':[{'label':'safe','points':p,'result':'safe'} for p in [3,1,0]]}
            rev=await s.update(1,10,rev,state)
            rev,state=await c.advance(1,10,rev,state,'0')
        assert state['stage']=='boss'
        rev,state=await c.resolve_boss(1,10,rev,state)
    assert state['victory'] and len(state['bosses_beaten'])==11
    assert await s.leaderboard(10,1)==[(1,1,341)]

def test_invalid_model_scenarios_rejected():
    assert parse_scenario('not json') is None
    assert parse_scenario('{"scenario":"x","choices":[]}') is None

def test_controller_construction_does_not_require_or_capture_an_event_loop():
    asyncio.set_event_loop(None)
    c=HarbingerController(None,None,None,'fake',None,None)
    assert c.slots is None and c.slot_loop is None

@run
async def test_migration_idempotent_and_legacy_progress_is_private(tmp_path):
    path=tmp_path/'local.db'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE rpg_state(user_id INTEGER PRIMARY KEY,bosses_beaten TEXT,scenario_data TEXT,current_boss INTEGER)')
        db.execute("INSERT INTO rpg_state VALUES(1,'[11]','{}',1)")
    mem,s,c=await setup(tmp_path)
    old=await s.get(1,0)
    assert old[1]['bosses_beaten']==[11]
    assert await s.get(1,10) is None
    await mem.init()
    assert await s.get(1,0)==old
    await s.forget(1)
    await mem.init()
    assert await s.get(1,0) is None

@pytest.mark.parametrize('old_round,expected_round,expected_stage',[(0,1,None),(10,10,'dice'),(11,11,'boss')])
@run
async def test_legacy_round_boundaries_do_not_replay_choices(tmp_path,old_round,expected_round,expected_stage):
    with sqlite3.connect(tmp_path/'local.db') as db:
        db.execute('CREATE TABLE rpg_state(user_id INTEGER PRIMARY KEY,bosses_beaten TEXT,scenario_data TEXT,current_round INTEGER,active INTEGER,world_type TEXT,char_type TEXT)')
        db.execute("INSERT INTO rpg_state VALUES(1,'[]','{}',?,1,'horror','transmigrated')",(old_round,))
    m,s,c=await setup(tmp_path)
    _,state=await s.get(1,0)
    assert state['current_round']==expected_round
    assert state.get('stage')==expected_stage
